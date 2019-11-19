# -*- coding: utf-8 -*-
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

from eggroll.core.meta_model import ErEndpoint, ErServerNode, ErServerCluster
from eggroll.core.meta_model import ErStore, ErStoreLocator
from eggroll.cluster_manager.cluster_manager_client import ClusterManagerClient

import unittest

class TestClusterManagerClient(unittest.TestCase):
  cluster_manager_client = ClusterManagerClient(opt = {'cluster_manager_host': 'localhost', 'cluster_manager_port': 4670})
  def test_get_server_node(self):
    result = TestClusterManagerClient.cluster_manager_client.get_server_node(ErServerNode(id=1))
    print(result)