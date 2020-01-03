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
import os

from eggroll.core.client import ClusterManagerClient
from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import SessionStatus, ProcessorTypes
from eggroll.core.meta_model import ErSessionMeta, \
    ErPartition
from eggroll.core.utils import get_self_ip, time_now, DEFAULT_DATETIME_FORMAT
from eggroll.utils.log_utils import get_logger

L = get_logger()


def session_init(session_id, options={"eggroll.session.deploy.mode": "standalone"}):
    er_session = ErSession(session_id=session_id, options=options)
    return er_session


class ErSession(object):
    def __init__(self,
            session_id=f'er_session_py_{time_now(format=DEFAULT_DATETIME_FORMAT)}_{get_self_ip()}',
            name='',
            tag='',
            processors=list(),
            options={}):
        self.__session_id = session_id
        self.__options = options.copy()
        self.__options[SessionConfKeys.CONFKEY_SESSION_ID] = self.__session_id
        self._cluster_manager_client = ClusterManagerClient(options=options)
        self._table_recorder = None

        if "EGGROLL_DEBUG" not in os.environ:
            os.environ['EGGROLL_DEBUG'] = "0"

        self.__eggroll_home = os.getenv('EGGROLL_HOME', None)
        if not self.__eggroll_home:
            raise EnvironmentError('EGGROLL_HOME is not set')

        self.__is_standalone = options.get(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE, "") == "standalone"
        if self.__is_standalone and os.name != 'nt':
            port = int(options.get('eggroll.resourcemanager.standalone.port', "4670"))
            startup_command = f'bash {self.__eggroll_home}/bin/eggroll_boot_standalone.sh -p {port} -s {self.__session_id}'
            import subprocess
            import atexit

            bootstrap_log_dir = f'{self.__eggroll_home}/logs/eggroll/'
            os.makedirs(bootstrap_log_dir, mode=0o755, exist_ok=True)
            with open(f'{bootstrap_log_dir}/standalone-manager.out', 'a+') as outfile, \
                    open(f'{bootstrap_log_dir}/standalone-manager.err', 'a+') as errfile:
                L.info(f'start up command: {startup_command}')
                manager_process = subprocess.run(startup_command.split(), stdout=outfile, stderr=errfile)
                returncode = manager_process.returncode
                L.info(f'start up returncode: {returncode}')

            def shutdown_standalone_manager(port, session_id, log_dir):
                shutdown_command = f"ps aux | grep eggroll | grep Bootstrap | grep '{port}' | grep '{session_id}' | grep -v grep | awk '{{print $2}}' | xargs kill"
                L.info(f'shutdown command: {shutdown_command}')
                with open(f'{log_dir}/standalone-manager.out', 'a+') as outfile, open(f'{log_dir}/standalone-manager.err', 'a+') as errfile:
                    manager_process = subprocess.check_output(shutdown_command, shell=True)
                    L.info(manager_process)

            atexit.register(shutdown_standalone_manager, port, self.__session_id, bootstrap_log_dir)

        session_meta = ErSessionMeta(id=self.__session_id,
                                     name=name,
                                     status=SessionStatus.NEW,
                                     tag=tag,
                                     processors=processors,
                                     options=options)

        from time import monotonic, sleep
        timeout = int(options.get("eggroll.session.create.timeout.ms", "5000")) / 1000
        endtime = monotonic() + timeout

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

        self.__cleanup_tasks = list()
        self.__processors = self.__session_meta._processors

        L.info('session init finished')

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
                raise ValueError(f"processor type {processor_type} not supported in roll pair")

    def route_to_egg(self, partition: ErPartition):
        target_server_node = partition._processor._server_node_id
        target_egg_processors = len(self._eggs[target_server_node])
        target_processor = (partition._id // target_egg_processors) % target_egg_processors

        return self._eggs[target_server_node][target_processor]

    def stop(self):
        return self._cluster_manager_client.stop_session(self.__session_meta)

    def set_table_recorder(self, roll_pair_contex):
        self._table_recorder = roll_pair_contex.load(name='__gc__' + self.__session_id, namespace=self.__session_id)

    def get_table_recorder(self):
        return self._table_recorder

    def get_session_id(self):
        return self.__session_id

    def get_session_meta(self):
        return self.__session_meta

    def add_cleanup_task(self, func):
        self.__cleanup_tasks.append(func)

    def run_cleanup_tasks(self):
        for func in self.__cleanup_tasks:
            func()

    def get_option(self, key, default=None):
        return self.__options.get(key, default)

    def has_option(self, key):
        return self.__options.get(key) is not None

    def get_all_options(self):
        return self.__options.copy()
