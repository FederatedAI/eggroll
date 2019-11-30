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

from eggroll.core.meta_model import ErServerNode, ErServerCluster, ErProcessor, ErProcessorBatch, ErSessionMeta
from eggroll.core.client import ClusterManagerClient, NodeManagerClient
from eggroll.core.utils import get_self_ip, time_now
from eggroll.core.constants import SessionStatus, ServerNodeStatus, ServerNodeTypes

class ErClientContext(object):
  def __init__(self, session_meta: ErSessionMeta, servicers: ErProcessorBatch, eggs: ErProcessorBatch):
    self.__session_meta = session_meta
    self.__servicers = servicers
    self.__eggs = eggs

  def create_roll_client(self):
    # todo: return roll object client

class ErClientSession(object):
  def __init__(self, session_id = None, name = '', tag = '', options = {}):
    if session_id:
      self.__session_id = session_id
    else:
      self.__session_id = f'er_client_session_{time_now()}_{get_self_ip()}'
    self.__cluster_manager_client = ClusterManagerClient({
      'cluster_manager_host': 'localhost',
      'cluster_manager_port': 4670,
    })

    self.__name = ''
    self.__options = options.copy()
    self.__status = SessionStatus.NEW
    self.__tag = tag
    self.__contexts = {}
    self.__server_cluster = self.get_server_cluster()

    self.__cleanup_tasks = []

  def get_server_cluster(self):
    healthy_node_example = ErServerNode(status = ServerNodeStatus.HEALTHY, node_type = ServerNodeTypes.NODE_MANAGER)

    return self.__cluster_manager_client.get_server_nodes(healthy_node_example)

  # todo: options or all options in session meta?
  def deploy(self, processor_type):
    # todo: create deployer with reflection
    if processor_type == ProcessorTypes.ROLL_PAIR:
      deployer = RollPairDeployer(session_meta = self.get_session_meta(),
                                  roll_servicer_cluster = self.__server_cluster,
                                  egg_cluster = self.__server_cluster,
                                  options = self.__options)
    else:
      raise NotImplementedError(f'processor type {processor_type} is not implemented yet')

    context = deployer.deploy()
    self.__contexts[processor_type] = context

    return context

  def get_session_meta(self):
    return ErSessionMeta(id = self.__session_id,
                         name = self.__name,
                         status = self.__status,
                         options = self.__options,
                         tag = self.__tag)

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
    return self.get_conf(key) is not None


class RollPairDeployer(object):
  def __init__(self, session_meta: ErSessionMeta, roll_servicer_cluster: ErServerCluster, egg_cluster: ErServerCluster, options = {}):
    self.__session_meta = session_meta
    self.__roll_servicer_cluster = roll_servicer_cluster
    self.__egg_cluster = egg_cluster

  def deploy(self):
    servicers = self.create_servicer()
    eggs = self.create_eggs()
    return self.create_context(session_meta=self.__session_meta, servicers=servicers, eggs=eggs)

  def create_servicer(self):
    target_node = self.__roll_servicer_cluster._server_nodes[0]
    nm_client = NodeManagerClient({'node_manager_host': target_node._endpoint._host, 'node_manager_port': target_node._endpoint._port})

    return nm_client.get_or_create_servicer(self.__session_meta)

  def create_eggs(self):
    target_nodes = self.__egg_cluster._server_nodes

    node_id_to_processor_batch = {}
    for node in target_nodes:
      nm_client = NodeManagerClient({'nm_manager_host': node._endpoint._host, 'node_manager_port': node._endpoint._port})
      node_id_to_processor_batch[node._id] = nm_client.get_or_create_processor_batch(self.__session_meta)

  def create_context(self, session_meta, servicers, eggs):
    return ErClientContext(session_meta=session_meta, servicers=servicers, eggs=eggs)




