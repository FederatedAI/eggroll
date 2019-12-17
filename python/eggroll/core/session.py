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
import argparse
import signal
import threading, os
import time
from sys import platform

from eggroll.core.meta_model import ErServerNode, ErServerCluster, ErProcessor, ErProcessorBatch, ErSessionMeta, \
    ErEndpoint
from eggroll.core.client import ClusterManagerClient, NodeManagerClient
from eggroll.core.utils import get_self_ip, time_now
from eggroll.core.constants import SessionStatus, ProcessorStatus, ServerNodeTypes, RollTypes, ProcessorTypes
from eggroll.core.conf_keys import ClusterManagerConfKeys, DeployConfKeys, SessionConfKeys

from eggroll.core.conf_keys import ClusterManagerConfKeys, DeployConfKeys
from eggroll.roll_pair.egg_pair import serve

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
        print ("StandaloneThread start：" + self.name)
        os.system(self.boot + " start '" + self.standalone + "' " + self.pname)
        print ("StandaloneThread stop：" + self.name)

    def stop(self):
        os.system(self.boot + " stop '" + self.standalone + "' " + self.pname)


class ErDeploy:
    pass


class ErStandaloneDeploy(ErDeploy):
    def __init__(self, session_meta: ErSessionMeta, options={}):
        self.manager_port = options.get("eggroll.standalone.manager.port", 4670)
        self.egg_ports = [int(v) for v in options.get("eggroll.standalone.egg.ports", "20001").split(",")]
        self.egg_transfer_ports = [int(v) for v in options.get("eggroll.standalone.egg.transfer.ports", "20002").split(",")]
        self._eggs = {0:[]}
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

        self._eggs[0].append(ErProcessor(id=0,
                                         server_node_id=0,
                                         processor_type=ProcessorTypes.EGG_PAIR,
                                         status=ProcessorStatus.RUNNING,
                                         command_endpoint=ErEndpoint("localhost", self.egg_ports[0]),
                                         transfer_endpoint=ErEndpoint("localhost", self.egg_transfer_ports[0])))

        self._rolls = [ErProcessor(id=0,
                                   server_node_id=0,
                                   processor_type=ProcessorTypes.ROLL_PAIR_SERVICER,
                                   status=ProcessorStatus.RUNNING,
                                   command_endpoint=ErEndpoint("localhost", self.manager_port))]

        processorBatch = ErProcessorBatch(id=0, name='standalone', processors=[self._rolls[0]] + list(self._eggs[0]))
        self.cm_client = ClusterManagerClient(options=options)

        self.cm_client.register_session(session_meta, processorBatch)

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
            elif processor_type == ProcessorTypes.ROLL_PAIR_SERVICER:
                self._rolls.append(processor)
            else:
                raise ValueError(f'processor type {processor_type} is unknown')

class ErSession(object):
    def __init__(self, session_id=None, name='', tag='', options={}):
        if session_id:
            self.__session_id = session_id
        else:
            self.__session_id = f'er_session_{time_now()}_{get_self_ip()}'
        self.__name = ''
        self.__options = options.copy()
        self.__options[SessionConfKeys.CONFKEY_SESSION_ID] = self.__session_id
        self.__status = SessionStatus.NEW
        self.__tag = tag
        self.session_meta = ErSessionMeta(id=self.__session_id,
                                          name=self.__name,
                                          status=self.__status,
                                          options=self.__options,
                                          tag=self.__tag)
        if self.get_option(DeployConfKeys.CONFKEY_DEPLOY_MODE) == "standalone":
            self.deploy_client = ErStandaloneDeploy(self.session_meta, options=options)
        else:
            self.deploy_client = ErClusterDeploy(self.session_meta, options=options)
        self._rolls = self.deploy_client._rolls
        self._eggs = self.deploy_client._eggs
        #print(f'eggs: {self._eggs}, rolls: {self._rolls}')

        self.cm_client = self.deploy_client.cm_client
        self.__cleanup_tasks = []

        print('session init finished')

    def get_session_id(self):
        return self.__session_id

    def add_cleanup_task(self, func):
        self.__cleanup_tasks.add(func)

    def run_cleanup_tasks(self):
        for func in self.__cleanup_tasks:
            func()

    def get_option(self, key, default=None):
        return self.__options.get(key, default)

    def has_option(self, key):
        return self.__options.get(key) is not None

    def get_all_options(self):
        return self.__options.copy()








