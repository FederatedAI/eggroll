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
import signal
import threading
import time
from copy import copy

from eggroll.core.client import ClusterManagerClient
from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import SessionStatus, ProcessorStatus, \
    ProcessorTypes
from eggroll.core.meta_model import ErProcessor, ErSessionMeta, \
    ErEndpoint, ErPartition
from eggroll.core.utils import get_self_ip, time_now

# TODO:1: support windows
# TODO:0: remove
if "EGGROLL_STANDALONE_DEBUG" not in os.environ:
    os.environ['EGGROLL_STANDALONE_DEBUG'] = "1"

class StandaloneThread(threading.Thread):
    def __init__(self, session_id="sid1", manager_port=4670, egg_port=20001, egg_transfer_port=20002):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.eggroll_home = "."
        if "EGGROLL_HOME" in os.environ:
            self.eggroll_home = os.environ["EGGROLL_HOME"]
        else:
            self.eggroll_home = "./"
        self.boot = self.eggroll_home + "/bin/eggroll_boot.sh"
        print("aa", self.boot)
        self.standalone = f"{self.eggroll_home}/bin/eggroll_boot_standalone.sh -p \
                          {manager_port} -e {egg_port} -t {egg_transfer_port} -s {session_id}"
        self.pname = str(session_id) + "-standalone"

    def run(self):
        print("StandaloneThread start：" + self.name)
        os.system(self.boot + " start '" + self.standalone + "' " + self.pname)
        print("StandaloneThread stop：" + self.name)

    def stop(self):
        os.system(self.boot + " stop '" + self.standalone + "' " + self.pname)


class ErDeploy:
    pass

class ErStandaloneDeploy(ErDeploy):
    def __init__(self, session_meta: ErSessionMeta, options={}):
        self.manager_port = options.get("eggroll.standalone.manager.port", 4670)
        self.egg_ports = [int(v) for v in options.get("eggroll.standalone.egg.ports", "20001").split(",")]
        self.egg_transfer_ports = [int(v) for v in options.get("eggroll.standalone.egg.transfer.ports", "20002").split(",")]
        self.self_server_node_id = int(options.get("eggroll.standalone.server.node.id", "2"))
        self._eggs = {self.self_server_node_id:[]}
        if len(self.egg_ports) > 1:
            raise NotImplementedError()
        if not ("EGGROLL_STANDALONE_DEBUG" in os.environ and os.environ['EGGROLL_STANDALONE_DEBUG'] == "1"):
            self.standalone_thread = StandaloneThread(session_meta._id, self.manager_port, self.egg_ports[0])
            self.standalone_thread.start()
            print("standalone_thread start", self.standalone_thread)
            signal.signal(signal.SIGTERM, self.stop)
            signal.signal(signal.SIGINT, self.stop)
            # TODO:0: more general
            time.sleep(5)

        self._eggs[self.self_server_node_id].append(ErProcessor(id=1,
                                         server_node_id=self.self_server_node_id,
                                         processor_type=ProcessorTypes.EGG_PAIR,
                                         status=ProcessorStatus.RUNNING,
                                         command_endpoint=ErEndpoint("localhost", self.egg_ports[0]),
                                         transfer_endpoint=ErEndpoint("localhost", self.egg_transfer_ports[0])))

        self._rolls = [ErProcessor(id=1,
                                   server_node_id=self.self_server_node_id,
                                   processor_type=ProcessorTypes.ROLL_PAIR_MASTER,
                                   status=ProcessorStatus.RUNNING,
                                   command_endpoint=ErEndpoint("localhost", self.manager_port))]

        self._processors = [self._rolls[0]] + list(self._eggs[self.self_server_node_id])

        session_meta_with_processor = copy(session_meta)
        session_meta_with_processor._processors = self._processors
        self.cm_client = ClusterManagerClient(options=options)

        self.cm_client.register_session(session_meta_with_processor)

    def stop(self):
        if self.standalone_thread:
            self.standalone_thread.stop()


class ErClusterDeploy(ErDeploy):
    def __init__(self, session_meta: ErSessionMeta, options={}):
        self.cm_client = ClusterManagerClient(options=options)
        self.session_meta = session_meta
        print(f'session_meta: {session_meta}')
        processor_batch = self.cm_client.get_or_create_session(self.session_meta)

        self._rolls = list()
        self._eggs = dict()
        for processor in processor_batch._processors:
            processor_type = processor._processor_type
            if processor_type == ProcessorTypes.EGG_PAIR:
                node_id = processor._server_node_id
                if node_id not in self._eggs.keys():
                    self._eggs[node_id] = list()

                node_eggs = self._eggs.get(node_id)
                node_eggs.append(processor)
            elif processor_type == ProcessorTypes.ROLL_PAIR_MASTER:
                self._rolls.append(processor)
            else:
                raise ValueError(f'processor type {processor_type} is unknown')


class ErSession(object):
    def __init__(self,
            session_id=f'er_session_py_{time_now()}_{get_self_ip()}',
            name='',
            tag='',
            processors=list(),
            options={}):
        self.__session_id = session_id
        self.__options = options.copy()
        self.__options[SessionConfKeys.CONFKEY_SESSION_ID] = self.__session_id
        self._cluster_manager_client = ClusterManagerClient(options=options)

        session_meta = ErSessionMeta(id=self.__session_id,
                                     name=name,
                                     status=SessionStatus.NEW,
                                     tag=tag,
                                     processors=processors,
                                     options=options)
        if not processors:
            self.__session_meta = self._cluster_manager_client.get_or_create_session(session_meta)
        else:
            self.__session_meta = self._cluster_manager_client.register_session(session_meta)

        self.__cleanup_tasks = list()
        self.__processors = self.__session_meta._processors

        print('session init finished')

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








