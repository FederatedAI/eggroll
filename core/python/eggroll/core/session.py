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

class ErClientContext(object):
  def __init__(self, session_meta: ErSessionMeta, servicers: ErProcessorBatch, eggs: ErProcessorBatch):
    self.__session_meta = session_meta
    self.__servicers = servicers
    self.__eggs = eggs

  def create_roll_client(self):
    # todo: return roll object client

class ErClientSession(object):
  def __init__(self, options = {}):
    self.__cluster_manager_client = ClusterManagerClient({
      'cluster_manager_host': 'localhost',
      'cluster_manager_port': 4670,
    })


  def deploy(self, processor_type, options = {}):
    if processor_type == ProcessorTypes.ROLL_PAIR:
      deployer = RollPairDeployer(self.get_session_meta())
    # todo: deploy, getting an context and put into dict

  # todo: generate session_meta
  def get_session_meta(self):
    return ErSessionMeta()


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




