#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import configparser
import os
from concurrent.futures import wait, FIRST_EXCEPTION
from copy import deepcopy

from eggroll.core.client import ClusterManagerClient
from eggroll.core.client import CommandClient
from eggroll.core.command.command_model import CommandURI
from eggroll.core.conf_keys import CoreConfKeys
from eggroll.core.conf_keys import SessionConfKeys, ClusterManagerConfKeys
from eggroll.core.constants import SessionStatus, ProcessorTypes, DeployModes
from eggroll.core.meta_model import ErJob, ErTask
from eggroll.core.meta_model import ErSessionMeta, ErPartition, ErStore
from eggroll.core.utils import generate_task_id
from eggroll.core.utils import get_self_ip, time_now, DEFAULT_DATETIME_FORMAT
from eggroll.core.utils import get_stack
from eggroll.core.utils import get_static_er_conf, set_static_er_conf
from eggroll.utils.log_utils import get_logger

L = get_logger()


def session_init(session_id, options={"eggroll.session.deploy.mode": "standalone"}):
    er_session = ErSession(session_id=session_id, options=options)
    return er_session


class ErSession(object):
    def __init__(self,
            session_id=None,
            name='',
            tag='',
            processors: list = None,
            options: dict = None):
        if processors is None:
            processors = []
        if options is None:
            options = {}
        if not session_id:
            self.__session_id = f'er_session_py_{time_now(format=DEFAULT_DATETIME_FORMAT)}_{get_self_ip()}'
        else:
            self.__session_id = session_id

        self.__eggroll_home = os.getenv('EGGROLL_HOME', None)
        if not self.__eggroll_home:
            raise EnvironmentError('EGGROLL_HOME is not set')

        if "EGGROLL_DEBUG" not in os.environ:
            os.environ['EGGROLL_DEBUG'] = "0"

        conf_path = options.get(CoreConfKeys.STATIC_CONF_PATH, f"{self.__eggroll_home}/conf/eggroll.properties")

        L.info(f"static conf path: {conf_path}")
        configs = configparser.ConfigParser()
        configs.read(conf_path)
        set_static_er_conf(configs['eggroll'])
        static_er_conf = get_static_er_conf()

        self.__options = options.copy()
        self.__options[SessionConfKeys.CONFKEY_SESSION_ID] = self.__session_id
        self._cluster_manager_client = ClusterManagerClient(options=options)

        self.__is_standalone = options.get(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE, "") == DeployModes.STANDALONE
        if self.__is_standalone and os.name != 'nt' and not processors and os.environ.get("EGGROLL_RESOURCE_MANAGER_AUTO_BOOTSTRAP", "1") == "1":
            port = int(options.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT,
                                   static_er_conf.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, "4670")))
            startup_command = f'bash {self.__eggroll_home}/bin/eggroll_boot_standalone.sh -c {conf_path} -s {self.__session_id}'
            import subprocess
            import atexit

            bootstrap_log_dir = f'{self.__eggroll_home}/logs/eggroll/'
            os.makedirs(bootstrap_log_dir, mode=0o755, exist_ok=True)
            with open(f'{bootstrap_log_dir}/standalone-manager.out', 'a+') as outfile, \
                    open(f'{bootstrap_log_dir}/standalone-manager.err', 'a+') as errfile:
                L.info(f'start up command: {startup_command}')
                manager_process = subprocess.run(startup_command, shell=True,  stdout=outfile, stderr=errfile)
                returncode = manager_process.returncode
                L.info(f'start up returncode: {returncode}')

            def shutdown_standalone_manager(port, session_id, log_dir):
                shutdown_command = f"ps aux | grep eggroll | grep Bootstrap | grep '{port}' | grep '{session_id}' | grep -v grep | awk '{{print $2}}' | xargs kill"
                L.info(f'shutdown command: {shutdown_command}')
                with open(f'{log_dir}/standalone-manager.out', 'a+') as outfile, open(f'{log_dir}/standalone-manager.err', 'a+') as errfile:
                    manager_process = subprocess.run(shutdown_command, shell=True, stdout=outfile, stderr=errfile)
                    returncode = manager_process.returncode
                    L.info(f'shutdown returncode: {returncode}')

            atexit.register(shutdown_standalone_manager, port, self.__session_id, bootstrap_log_dir)

        session_meta = ErSessionMeta(id=self.__session_id,
                                     name=name,
                                     status=SessionStatus.NEW,
                                     tag=tag,
                                     processors=processors,
                                     options=options)

        from time import monotonic, sleep
        timeout = int(SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get_with(options)) / 1000 + 2
        endtime = monotonic() + timeout

        # TODO:0: ignores exception while starting up in standalone mod
        while True:
            try:
                if not processors:
                    self.__session_meta = self._cluster_manager_client.get_or_create_session(session_meta)
                else:
                    self.__session_meta = self._cluster_manager_client.register_session(session_meta)
                break
            except:
                if monotonic() < endtime:
                    sleep(0.1)
                else:
                    raise

        self.__exit_tasks = list()
        self.__processors = self.__session_meta._processors

        L.info(f'session init finished: {self.__session_id}, details: {self.__session_meta}')
        self.stopped = self.__session_meta._status == SessionStatus.CLOSED or self.__session_meta._status == SessionStatus.KILLED
        self._rolls = list()
        self._eggs = dict()

        for processor in self.__session_meta._processors:
            processor_type = processor._processor_type
            if processor_type == ProcessorTypes.EGG_PAIR:
                server_node_id = processor._server_node_id
                if server_node_id not in self._eggs:
                    self._eggs[server_node_id] = list()
                self._eggs[server_node_id].append(processor)
            elif processor_type == ProcessorTypes.ROLL_PAIR_MASTER:
                self._rolls.append(processor)
            else:
                raise ValueError(f'processor type {processor_type} not supported in roll pair')

    def get_rank_in_node(self, partition_id, server_node_id):
        processor_count_of_node = len(self._eggs[server_node_id])
        node_count = len(self._eggs)
        rank_in_node = (partition_id // node_count) % processor_count_of_node

        return rank_in_node

    def route_to_egg(self, partition: ErPartition):
        server_node_id = partition._processor._server_node_id
        rank_in_node = partition._rank_in_node
        if partition._rank_in_node is None or rank_in_node < 0:
            rank_in_node = self.get_rank_in_node(partition_id=partition._id,
                                                 server_node_id=server_node_id)

        result = self.route_to_egg_by_rank(server_node_id, rank_in_node)

        return result

    def route_to_egg_by_rank(self, server_node_id, rank_in_node):
        result = self._eggs[server_node_id][rank_in_node]
        if not result._command_endpoint._host or result._command_endpoint._port <= 0:
            raise ValueError(f'error routing to egg: {result} in session: {self.__session_id}')

        return result

    def populate_processor(self, store: ErStore):
        populated_partitions = list()
        for p in store._partitions:
            server_node_id = p._processor._server_node_id
            rank_in_node = self.get_rank_in_node(p._id, p._processor._server_node_id)
            pp = ErPartition(id=p._id,
                             store_locator=p._store_locator,
                             processor=self.route_to_egg_by_rank(server_node_id, rank_in_node),
                             rank_in_node=rank_in_node)
            populated_partitions.append(pp)
        return ErStore(store_locator=store._store_locator, partitions=populated_partitions, options=store._options)

    def submit_job(self,
            job: ErJob,
            output_types: list = None,
            command_uri: CommandURI = None,
            create_output_if_missing=True):
        if not output_types:
            output_types = [ErTask]
        final_job = self.populate_output_store(job) if create_output_if_missing else job
        tasks = self._decompose_job(final_job)
        command_client = CommandClient()
        return command_client.async_call(args=tasks, output_types=output_types, command_uri=command_uri)

    def wait_until_job_finished(self, task_futures: list, timeout=None, return_when=FIRST_EXCEPTION):
        return wait(task_futures, timeout=timeout, return_when=return_when).done

    def _decompose_job(self, job: ErJob):
        input_total_partitions = job._inputs[0]._store_locator._total_partitions
        output_total_partitions = 0 \
            if not job._outputs \
            else job._outputs[0]._store_locator._total_partitions

        larger_total_partitions = max(input_total_partitions, output_total_partitions)

        populated_input_partitions = self.populate_processor(job._inputs[0])._partitions

        if output_total_partitions > 0:
            populated_output_partitions = self.populate_processor(job._outputs[0])._partitions
        else:
            populated_output_partitions = list()

        result = list()
        for i in range(larger_total_partitions):
            input_partitions = list()
            output_partitions = list()

            if i < input_total_partitions:
                input_processor = populated_input_partitions[i]._processor
                input_server_node_id = input_processor._server_node_id
                for input_store in job._inputs:
                    input_partitions.append(ErPartition(
                            id=i,
                            store_locator=input_store._store_locator,
                            processor=input_processor))
            else:
                input_processor = None
                input_server_node_id = None

            if i < output_total_partitions:
                output_processor = populated_output_partitions[i]._processor
                output_server_node_id = output_processor._server_node_id
                for output_store in job._outputs:
                    output_partitions.append(ErPartition(
                            id=i,
                            store_locator=output_store._store_locator,
                            processor=output_processor))
            else:
                output_processor = None
                output_server_node_id = None

            task = [ErTask(id=generate_task_id(job._id, i),
                           name=f'{job._name}',
                           inputs=input_partitions,
                           outputs=output_partitions,
                           job=job)]
            if input_server_node_id == output_server_node_id:
                result.append(
                        (task, input_processor._command_endpoint))
            else:
                if input_server_node_id is not None:
                    result.append(
                            (task, input_processor._command_endpoint))
                if output_server_node_id is not None:
                    result.append(
                            (task, output_processor._command_endpoint))

        return result

    def populate_output_store(self, job: ErJob):
        is_output_blank = not job._outputs or not job._outputs[0]
        is_output_not_populated = is_output_blank or not job._outputs[0]._partitions
        if is_output_not_populated:
            if is_output_blank:
                final_output_proposal = job._inputs[0].fork()
            else:
                final_output_proposal = job._outputs[0]

            refresh_nodes = job._options.get('refresh_nodes', False)
            if refresh_nodes:
                final_output_proposal._partitions = []
        else:
            final_output_proposal = job._outputs[0]

        final_output = self.populate_processor(
                self._cluster_manager_client.get_or_create_store(final_output_proposal))

        if final_output._store_locator._total_partitions != \
                final_output_proposal._store_locator._total_partitions:
            raise ValueError(f'partition count of actual output and proposed output does not match. '
                             f'actual={final_output}, proposed={final_output_proposal}')
        final_job = deepcopy(job)
        final_job._outputs = [final_output]

        return final_job

    def stop(self):
        L.info(f'stopping session (gracefully): {self.__session_id}')
        L.debug(f'stopping session (gracefully), details: {self.__session_meta}')
        L.debug(f'stopping (gracefully) for {self.__session_id} from: {get_stack()}')
        self.run_exit_tasks()
        self.stopped = True
        return self._cluster_manager_client.stop_session(self.__session_meta)

    def kill(self):
        L.info(f'killing session (forcefully): {self.__session_id}')
        L.debug(f'killing session (forcefully), details: {self.__session_meta}')
        L.debug(f'killing (forcefully) for {self.__session_id} from: {get_stack()}')
        self.stopped = True
        return self._cluster_manager_client.kill_session(self.__session_meta)

    def get_session_id(self):
        return self.__session_id

    def get_session_meta(self):
        return self.__session_meta

    # todo:1: add_exit_task? not necessarily a cleanup semantic
    def add_exit_task(self, func):
        self.__exit_tasks.append(func)

    def run_exit_tasks(self):
        L.debug(f'running exit tasks: {self.__session_id}')
        for func in self.__exit_tasks:
            func()

    def get_option(self, key, default=None):
        return self.__options.get(key, default)

    def has_option(self, key):
        return self.__options.get(key) is not None

    def get_all_options(self):
        return self.__options.copy()

    def is_stopped(self):
        return self.stopped


class JobRunner(object):
    def __init__(self, session: ErSession):
        self._session = session

    def run(self, job: ErJob):
        tasks = self.decompose_job()



