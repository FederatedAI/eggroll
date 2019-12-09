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
          'pair_type': 'v1/roll-pair',
          'roll_pair_service_host': 'localhost',
          'roll_pair_service_port': 20000,
          'egg_pair_service_host': 'localhost',
          'egg_pair_service_port': 20001}

  storage_options = {'cluster_manager_host': 'localhost',
          'cluster_manager_port': 4670,
          'pair_type': 'v1/egg-pair',
          'egg_pair_service_host': 'localhost',
          'egg_pair_service_port': 20001}

  store_type = StoreTypes.ROLLPAIR_LMDB

  #same as v1.x function table
  def test_pair_store(self):
    rp = RollPair(options=TestRollPair.options)
    # pair_store = rp.pair_store(name='pair_store', namespace='pair_store_namespace',
    #                            partition=10, persistent=True,
    #                            persistent_engine=TestRollPair.store_type)

    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace="load_namespace",
                                   name="name", total_partitions=10))

    res = rp.load(store)
    res.put(b'a', b'1')
    print(res)

  def test_destroy(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                   name="name"))
    rp = RollPair(store, options=TestRollPair.options)
    rp.destroy()
    print("destroy store:{}".format(store))

  def test_get(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                   name="name"))
    rp = RollPair(store, options=TestRollPair.options)
    res = rp.get(bytes('1', encoding='utf-8'))
    print("res: {}".format(res))

  def test_put(self):
    store = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                                 name="name"))
    rp = RollPair(er_store=store, options=TestRollPair.options)
    res = rp.put(b'key', b'value')
    print("res: {}".format(res))

  def test_map_values(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    res = rp.map_values(lambda v : v + b'~2', output=ErStore(store_locator = ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace', name='testMapValues')))

    print('res: ', res)

  def test_reduce(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))

    rp = RollPair(store, options=TestRollPair.options)
    res = rp.reduce(lambda x, y : x + y)
    print('res: ', res)

  def test_aggregate(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))

    rp = RollPair(store, options=TestRollPair.options)
    res = rp.aggregate(zero_value=None, seq_op=lambda x, y : x + y, comb_op=lambda x, y : y + x)
    print('res: ', res)

  def test_join(self):
    left_locator = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                   name='name'))
    right_locator = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                   name='test'))

    left = RollPair(left_locator, options=TestRollPair.options)
    right = RollPair(right_locator, options=TestRollPair.options)
    res = left.join(right, lambda x, y : x + b' joins ' + y)
    print('res: ', res)


  def test_map(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    res = rp.map(lambda k, v: (b'k_' + k, b'v_' + v), output=ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace', name='testMap')))

    print('res: ', res)

  def test_map_partitions(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    def func(iter):
      ret = []
      for k, v in iter:
        k = int(k)
        v = int(v)
        ret.append((bytes(f"{k}_{v}_0", encoding='utf8'), bytes(str(v ** 2), encoding='utf8')))
        ret.append((bytes(f"{k}_{v}_1", encoding='utf8'), bytes(str(v ** 3), encoding='utf8')))
      return ret
    res = rp.map_partitions(func)
    print("res: {}".format(res))

  def test_collapse_partitions(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    def f(iterator):
      sum = ""
      for k, v in iterator:
        sum += v
      return sum
    res = rp.collapse_partitions(f)
    print("res: {}".format(res))

  def test_filter(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)
    res = rp.filter(lambda k, v: int(k) % 2 != 0)
    print("res: {}".format(res))

  def test_glom(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    res = rp.glom()
    print("res: {}".format(res))

  def test_flatMap(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)
    import random
    def foo(k, v):
      k = int(k)
      v = int(v)
      result = []
      r = random.randint(10000, 99999)
      for i in range(0, k):
        result.append((k + r + i, v + r + i))
      return result
    res = rp.flat_map(foo)
    print("res: {}".format(res))

  def test_sample(self):
    store = ErStore(ErStoreLocator(store_type=TestRollPair.store_type, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, options=TestRollPair.options)

    res = rp.sample(0.1, 81)
    print("res: {}".format(res))

  def test_subtractByKey(self):
    left_locator = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                                        name='name'))
    right_locator = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                                         name='test'))

    left = RollPair(left_locator, options=TestRollPair.options)
    right = RollPair(right_locator, options=TestRollPair.options)

    res = left.subtract_by_key(right)
    print("res: {}".format(res))

  def test_union(self):
    left_locator = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                                        name='name'))
    right_locator = ErStore(store_locator=ErStoreLocator(store_type=TestRollPair.store_type, namespace="namespace",
                                                         name='test'))

    left = RollPair(left_locator, options=TestRollPair.options)
    right = RollPair(right_locator, options=TestRollPair.options)

    res = left.union(right, lambda v1, v2: v1 + v2)
    print("res: {}".format(res))

if __name__ == '__main__':
  unittest.main()
