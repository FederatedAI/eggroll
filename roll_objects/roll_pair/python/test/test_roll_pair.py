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

import grpc
import time
import unittest
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.core.constants import StoreTypes


class TestRollPair(unittest.TestCase):
  options = {'cluster_manager_host': 'localhost',
          'cluster_manager_port': 4670,
          'roll_pair_service_host': 'localhost',
          'roll_pair_service_port': 20000}
  def test_map_values(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    res = rp.map_values(lambda v : v + b'~2', output=ErStore(store_locator = ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace='namespace', name='testMapValues')))

    print('res: ', res)

  def test_reduce(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace='namespace',
                                   name='name'))

    rp = RollPair(store, options=TestRollPair.options)
    res = rp.reduce(lambda x, y : x + y)
    print('res: ', res)

  def test_aggregate(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace='namespace',
                                   name='name'))

    rp = RollPair(store, options=TestRollPair.options)
    res = rp.aggregate(zero_value=None, seq_op=lambda x, y : x + y, comb_op=lambda x, y : y + x)
    print('res: ', res)

  def test_join(self):
    left_locator = ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace="namespace",
                                   name='name')
    right_locator = ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace="namespace",
                                   name='test')

    left = RollPair(left_locator)
    right = RollPair(right_locator)
    res = left.join(right, lambda x, y : x + b' joins ' + y)
    print('res: ', res)


  def test_map(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    res = rp.map(lambda k, v: (b'k_' + k, b'v_' + v), output=ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace='namespace', name='testMap')))

    print('res: ', res)


if __name__ == '__main__':
  unittest.main()
