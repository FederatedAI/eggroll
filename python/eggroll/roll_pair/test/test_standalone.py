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

import unittest

from eggroll.roll_pair.test.roll_pair_test_assets import get_debug_test_context, \
    get_cluster_context, get_standalone_context

is_debug = True
is_standalone = False

class TestStandalone(unittest.TestCase):
    ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        if is_debug:
            cls.ctx = get_debug_test_context()
        else:
            if is_standalone:
                cls.ctx = get_standalone_context()
            else:
                cls.ctx = get_cluster_context()

    def setUp(self):
        self.ctx = TestStandalone.ctx

    @classmethod
    def tearDownClass(cls) -> None:
        if not is_debug:
            cls.ctx.get_session().stop()

    def test_parallelize(self):
        print(list(self.ctx.parallelize(range(15)).get_all()))

    def test_get(self):
        for i in range(10):
            self.ctx.load("ns1", "n26").put(f"k{i}", f"v{i}")
            print(self.ctx.load("ns1", "n26").get(f"k{i}"))

    def test_put_all(self):
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5"), ("k6", "v6")]
        #data = [("k1", "v1"), ("k2", "v2")]
        options = {}
        options['include_key'] = True
        self.ctx.load("ns1", "testPutAll").put_all(data, options=options)
        table = list(self.ctx.load("ns1", "testPutAll").get_all())
        print("get res:{}".format(table))

    def test_multi_partition_put_all(self):
        #data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5"), ("k6", "v6")]

        def kv_generator(limit):
            for i in range(limit):
                yield f"k{i}", f"v{i}"

        options = {}
        options['total_partitions'] = 3
        options['include_key'] = True
        table = self.ctx.load("ns1", "testMultiPartitionPutAll", options=options)
        table.put_all(kv_generator(100000), options=options)
        print(table.count())

    def test_get_all(self):
        table =self.ctx.load("ns1", "testMultiPartitionPutAll")
        print(str(table))
        res = table.get_all()
        print(list(res))

    def test_count(self):
        options = {}
        options['total_partitions'] = 3
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_count', options=options).put_all(data, options=options)
        print(f"count: {table.count()}")

    def test_delete(self):
        options = {}
        options['total_partitions'] = 1
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_delete_one', options=options).put_all(data, options=options)
        print("before delete:{}".format(list(table.get_all())))
        table.delete("k1")
        print("after delete:{}".format(list(table.get_all())))

    def test_destroy(self):
        options = {}
        options['total_partitions'] = 1
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_destroy', options=options).put_all(data, options=options)
        print("before destroy:{}".format(list(table.get_all())))
        table.destroy()
        # TODO:1: table which has been destroyed cannot get_all, should raise exception
        print("after destroy:{}".format((table.count())))

    def test_map_values(self):
        rp = self.ctx.load("ns1", "test_map_values").put_all(range(10))
        print(list(rp.map_values(lambda v: str(v) + 'map_values').get_all()))

    def test_map_partitions(self):
        data = [(str(i), i) for i in range(10)]
        rp = self.ctx.load("ns1", "test_map_partitions").put_all(data, options={"include_key": True})
        def func(iter):
            ret = []
            for k, v in iter:
                ret.append((f"{k}_{v}_0", v ** 2))
                ret.append((f"{k}_{v}_1", v ** 3))
            return ret
        print(list(rp.map_partitions(func).get_all()))

    def test_map(self):
        rp = self.ctx.load("ns1", "testMap").put_all(range(100))

        print(list(rp.map(lambda k, v: (k + 1, v)).get_all()))

    def test_multi_partition_map(self):
        options = {}
        options['total_partitions'] = 3
        options['include_key'] = True
        rp = self.ctx.load("ns1", "testMultiPartitionsMap", options=options).put_all(range(10000))

        result = rp.map(lambda k, v: (k + 1, v))
        print(result.count())

    def test_collapse_partitions(self):
        rp = self.ctx.load("ns1", "test_collapse_partitions").put_all(range(5))
        def f(iterator):
            sum = []
            for k, v in iterator:
                sum.append((k, v))
            return sum
        print(list(rp.collapse_partitions(f).get_all()))

    def test_filter(self):
        rp = self.ctx.load("ns1", "test_filter").put_all(range(5))
        print(list(rp.filter(lambda k, v: v % 2 != 0).get_all()))

    def test_flatMap(self):
        rp = self.ctx.load("ns1", "test_flat_map").put_all(range(5))
        import random
        def foo(k, v):
            result = []
            r = random.randint(10000, 99999)
            for i in range(0, k):
                result.append((k + r + i, v + r + i))
            return result
        print(list(rp.flat_map(foo).get_all()))

    def test_glom(self):
        rp = self.ctx.load("ns1", "test_glom").put_all(range(5))
        print(list(rp.glom().get_all()))

    def test_join(self):
        left_rp = self.ctx.load("ns1", "testJoinLeft").put_all([('a', 1), ('b', 4)], options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testJoinRight").put_all([('a', 2), ('c', 4)], options={"include_key": True})
        print(list(left_rp.join(right_rp, lambda v1, v2: v1 + v2).get_all()))

    def test_reduce(self):
        from operator import add
        rp = self.ctx.load("ns1", "testReduce").put_all(range(20))
        print(list(rp.reduce(add).get_all()))

    def test_multi_partition_reduce(self):
        from operator import add
        options = {}
        options['total_partitions'] = 3
        options['include_key'] = True
        rp = self.ctx.load("ns1", "testMultiPartitionReduce", options=options).put_all(range(20))
        print(list(rp.reduce(add).get_all()))

    def test_sample(self):
        rp = self.ctx.load("ns1", "testSample").put_all(range(100))
        print(6 <= rp.sample(0.1, 81).count() <= 14)

    def test_subtract_by_key(self):
        left_rp = self.ctx.load("namespace1206", "testSubtractByKeyLeft1206").put_all(range(10))
        right_rp = self.ctx.load("namespace1206", "testSubtractByKeyRight1206").put_all(range(5))
        print(list(left_rp.subtract_by_key(right_rp).get_all()))

    def test_union(self):
        left_rp = self.ctx.load("ns1", "testUnionLeft").put_all([1, 2, 3])
        right_rp = self.ctx.load("ns1", "testUnionRight").put_all([(1, 1), (2, 2), (3, 3)], options={"include_key": True})
        print(list(left_rp.union(right_rp, lambda v1, v2: v1 + v2).get_all()))

        left_rp = self.ctx.load("namespace1206", "testUnionLeft1206").put_all([1, 2, 3])
        right_rp = self.ctx.load("namespace1206", "testUnionRight1206").put_all([(1, 1), (2, 2), (3, 3)], options={"include_key": True})
        print("left:", list(left_rp.get_all()))
        print("right:", list(right_rp.get_all()))
        print(list(left_rp.union(right_rp, lambda v1, v2: v1 + v2).get_all()))

    def test_aggregate(self):
        from operator import add, mul
        options = {}
        options['total_partitions'] = 3

        rp = self.ctx.load("ns1", "testMultiPartitionAggregate", options=options)
        rp.put_all(range(10))
        print(list(rp.get_all()))
        print('count:', rp.count())
        print(list(rp.aggregate(zero_value=0, seq_op=add, comb_op=mul).get_all()))
