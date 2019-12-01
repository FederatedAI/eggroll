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

from eggroll.core.meta_model import ErServerNode, ErServerCluster, ErProcessor, ErProcessorBatch, ErSessionMeta, \
  ErEndpoint
from eggroll.core.client import ClusterManagerClient, NodeManagerClient
from eggroll.core.utils import get_self_ip, time_now
from eggroll.core.constants import SessionStatus, ServerNodeStatus, ServerNodeTypes, RollTypes, ProcessorTypes
from eggroll.core.conf_keys import NodeManagerConfKeys

class ErDeploy:
  pass

class ErStandaloneDeploy(ErDeploy):
  def __init__(self, session_meta: ErSessionMeta):
    self.processors = [
      ErProcessor(id=0, processor_type=ProcessorTypes.ROLL_PAIR_SERVICER, command_endpoint=ErEndpoint("localhost", 4671)),
      ErProcessor(id=0, processor_type=ProcessorTypes.EGG_PAIR, command_endpoint=ErEndpoint("localhost", 4671))
    ]
    self.cm_client = ClusterManagerClient({
      'cluster_manager_host': 'localhost',
      'cluster_manager_port': 4670,
    })


class ErClusterDeploy(ErDeploy):
  def __init__(self, session_meta: ErSessionMeta):
    self.cm_client = ClusterManagerClient({
      'cluster_manager_host': 'localhost',
      'cluster_manager_port': 4670,
    })
    self.session_meta = session_meta
    self.processors = self.cm_client.get_or_create_session(self.session_meta)


class ErSession(object):
  def __init__(self, session_id = None, name = '', tag = '', options = {}):
    if session_id:
      self.__session_id = session_id
    else:
      self.__session_id = f'er_session_{time_now()}_{get_self_ip()}'
    self.__name = ''
    self.__options = options.copy()
    self.__status = SessionStatus.NEW
    self.__tag = tag
    self.session_meta = ErSessionMeta(id = self.__session_id,
                                      name = self.__name,
                                      status = self.__status,
                                      options = self.__options,
                                      tag = self.__tag)
    if self.has_option("eggroll.deploy.mode") and self.get_option("eggroll.deploy.mode") == "standalone":
      self.deploy_client = ErStandaloneDeploy(self.session_meta)
    else:
      self.deploy_client = ErClusterDeploy(self.session_meta)
    self.processors = self.deploy_client.processors
    self.cm_client = self.deploy_client.cm_client
    self.__cleanup_tasks = []

  def get_session_id(self):
    return self.__session_id

  def add_cleanup_task(self, func):
      self.__cleanup_tasks.add(func)

  def run_cleanup_tasks(self):
    for func in self.__cleanup_tasks:
      func()

  def get_option(self, key):
    return self.__options.get(key)

  def has_option(self, key):
    return self.__options.get(key) is not None








