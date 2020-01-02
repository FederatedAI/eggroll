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
    get_cluster_context, get_standalone_context, set_default_option, \
    get_default_options

is_debug = True
is_standalone = False
total_partitions = 1


def kv_generator(limit):
    for i in range(limit):
        yield f"k{i}", f"v{i}"


def kv_list(limit):
    ret = []
    for i in range(limit):
         ret.append((f"k{i}", f"v{i}"))
    return ret


def get_value(roll_pair):
    return list(roll_pair.get_all())


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
        set_default_option('include_key', False)
        set_default_option('total_partitions', total_partitions)

    def setUp(self):
        self.ctx = TestStandalone.ctx

    def assertUnOrderListEqual(self, list1, list2):
        self.assertEqual(sorted(list1), sorted(list2))

    @staticmethod
    def str_generator(include_key=True, row_limit=10, key_suffix_size=0, value_suffix_size=0):
        for i in range(row_limit):
            if include_key:
                yield str(i) + "s"*key_suffix_size, str(i) + "s"*value_suffix_size
            else:
                yield str(i) + "s"*value_suffix_size

    @classmethod
    def tearDownClass(cls) -> None:
        if not is_debug:
            cls.ctx.get_session().stop()

    def test_parallelize(self):
        rp = self.ctx.parallelize(self.str_generator())
        self.assertUnOrderListEqual(((k, v) for k, v in self.str_generator()), rp.get_all())
        rp.destroy()

    def test_get(self):
        options = get_default_options()
        options['include_key'] = True
        for i in range(10):
            self.ctx.load("ns1", "testGet", options=options).put(f"k{i}", f"v{i}")
            print(self.ctx.load("ns1", "testGet").get(f"k{i}"))
            self.assertEqual(self.ctx.load("ns1", "testGet").get(f"k{i}"), f"v{i}")
        assert (self.ctx.load("ns1", "testGet").get(f"k{100}") == None)
        self.ctx.load("ns1", "testGet", options=options).destroy()

    def test_put_all(self):
        #data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5"), ("k6", "v6")]
        #data = [("k1", "v1"), ("k2", "v2")]
        options = get_default_options()
        t = self.ctx.load("ns1", "testPutAll", options=options)
        options['include_key'] = True
        t.put_all(((k, "a"*100) for k in range(1000)), options=options)

        self.assertEqual(t.count(), 100)
        self.assertUnOrderListEqual(t.get_all(), kv_list(100))
        t.destroy()

    def test_put_all_value(self):
        options = get_default_options()
        self.ctx.load("ns1", "testPutAllValue", options=options).destroy()
        options['include_key'] = False
        cnt = 100
        rp = self.ctx.load("ns1", "testPutAllValue", options=options).put_all(("s" for i in range(cnt)), options=options)
        self.assertEqual(rp.count(), cnt)

    def test_multi_partition_put_all(self):
        #data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5"), ("k6", "v6")]

        options = get_default_options()
        options['include_key'] = True
        table = self.ctx.load("ns1", "testMultiPartitionPutAll", options=options)
        table.put_all(kv_generator(100), options=options)
        self.assertEqual(table.count(), 100)
        self.assertEqual(get_value(table), kv_list(100))

    def test_get_all(self):
        options = get_default_options()
        table =self.ctx.load("ns1", "testMultiPartitionPutAll", options=options)
        print(str(table))
        res = table.get_all()
        print(list(res))
        self.assertEqual(table.count(), 100)
        self.assertEqual(get_value(table), kv_list(100))

    def test_count(self):
        options = {}
        options['total_partitions'] = 3
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_count', options=options).put_all(data, options=options)
        print(f"count: {table.count()}")
        self.assertEqual(table.count(), 4)

    def test_delete(self):
        options = get_default_options()
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_delete_one', options=options).put_all(data, options=options)
        print("before delete:{}".format(list(table.get_all())))
        table.delete("k1")
        print("after delete:{}".format(list(table.get_all())))
        self.assertEqual(get_value(table), ([("k2", "v2"), ("k3", "v3"), ("k4", "v4")]))

    def test_destroy(self):
        options = get_default_options()
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_destroy', options=options).put_all(data, options=options)
        print("before destroy:{}".format(list(table.get_all())))
        table.destroy()
        # TODO:1: table which has been destroyed cannot get_all, should raise exception
        print("after destroy:{}".format(list(table.get_all())))
        self.assertEqual(table.count(), 0)

    def test_take(self):
        options = get_default_options()
        options['keys_only'] = True
        table = self.ctx.load('ns1', 'test_take', options=options).put_all(range(10), options=options)
        print(table.take(n=3, options=options))
        self.assertEqual(table.take(n=3, options=options), [0, 1, 2])

        options_kv = get_default_options()
        options_kv['keys_only'] = False
        table = self.ctx.load('ns1', 'test_take_kv', options=options_kv).put_all(range(10), options=options_kv)
        print(table.take(n=3, options=options_kv))
        self.assertEqual(table.take(n=3, options=options_kv), [(0, 0), (1, 1), (2, 2)])

    def test_first(self):
        options = get_default_options()
        options['keys_only'] = True
        table = self.ctx.load('ns1', 'test_take', options=options).put_all(range(10), options=options)
        print(table.first(options=options))
        self.assertEqual(table.first(options=options), 0)

        options_kv = get_default_options()
        options_kv['keys_only'] = False
        table = self.ctx.load('ns1', 'test_take_kv', options=options_kv).put_all(range(10), options=options_kv)
        print(table.first(options=options_kv))
        self.assertEqual(table.first(options=options), (0, 0))

    def test_map_values(self):
        options = get_default_options()
        rp = self.ctx.load("ns1", "test_map_values", options=options).put_all(range(10), options=options)
        res = rp.map_values(lambda v: str(v) + 'map_values')
        print(list(res.get_all()))
        self.assertEqual(get_value(res), [(0, '0map_values'), (1, '1map_values'), (2, '2map_values'), (3, '3map_values'),
                                   (4, '4map_values'), (5, '5map_values'), (6, '6map_values'), (7, '7map_values'),
                                   (8, '8map_values'), (9, '9map_values')])

    def test_map_partitions(self):
        options = get_default_options()
        data = [(str(i), i) for i in range(10)]
        rp = self.ctx.load("ns1", "test_map_partitions", options=options).put_all(data, options={"include_key": True})
        def func(iter):
            ret = []
            for k, v in iter:
                ret.append((f"{k}_{v}_0", v ** 2))
                ret.append((f"{k}_{v}_1", v ** 3))
            return ret
        table = rp.map_partitions(func)
        print(list(rp.map_partitions(func).get_all()))
        self.assertEqual(get_value(table), [('0_0_0', 0), ('0_0_1', 0), ('1_1_0', 1), ('1_1_1', 1), ('2_2_0', 4), ('2_2_1', 8),
                                ('3_3_0', 9), ('3_3_1', 27), ('4_4_0', 16), ('4_4_1', 64), ('5_5_0', 25),
                                ('5_5_1', 125), ('6_6_0', 36), ('6_6_1', 216), ('7_7_0', 49), ('7_7_1', 343),
                                ('8_8_0', 64), ('8_8_1', 512), ('9_9_0', 81), ('9_9_1', 729)])

    def test_map(self):
        rp = self.ctx.load("ns1", "testMap2")
        rp.destroy()
        self.ctx.load("ns1", "testMap2").put_all(("s"*4 for i in range(1000)), options={"include_key": False})
        options = get_default_options()
        rp = self.ctx.load("ns1", "testMap2", options=options)
        # rp = self.ctx.load("ns1", "testMap3", {"store_type":StoreTypes.ROLLPAIR_CACHE})
        # rp.put_all(range(100*1000))
        # print(rp.count())
        # print(rp.map_values(lambda v: v))
        print(rp.map(lambda k, v: (k + 1, v)).count())
        self.assertEqual(get_value(rp.map(lambda k, v: (k + 1, v))).count(), 1000)
        # print(rp.first())
        # print(rp.map(lambda k, v: (k + 1, v)).count())

        # print(list(rp.map(lambda k, v: (k + 1, v)).get_all()))

    def test_multi_partition_map(self):
        options = get_default_options()
        options['total_partitions'] = 3
        options['include_key'] = False
        rp = self.ctx.load("ns1", "testMultiPartitionsMap", options=options).put_all(range(100), options=options)

        result = rp.map(lambda k, v: (k + 1, v))
        print(result.count())
        self.assertEqual(result.count(), 100)

    def test_collapse_partitions(self):
        options = get_default_options()
        rp = self.ctx.load("ns1", "test_collapse_partitions", options=options).put_all(range(5), options=options)

        def f(iterator):
            sum = []
            for k, v in iterator:
                sum.append((k, v))
            return sum
        print(list(rp.collapse_partitions(f).get_all()))
        self.assertEqual(get_value(rp.collapse_partitions(f)), [(4, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)])])


    def test_filter(self):
        options = get_default_options()
        rp = self.ctx.load("ns1", "test_filter", options=options).put_all(range(5), options=options)
        print(list(rp.filter(lambda k, v: v % 2 != 0).get_all()))
        self.assertEqual(get_value(rp.filter(lambda k, v: v % 2 != 0)), [(1, 1), (3, 3)])

    def test_flatMap(self):
        options = get_default_options()
        rp = self.ctx.load("ns1", "test_flat_map", options=options).put_all(range(5), options=options)
        import random

        def foo(k, v):
            result = []
            r = random.randint(10000, 99999)
            for i in range(0, k):
                result.append((k + r + i, v + r + i))
            return result
        print(list(rp.flat_map(foo).get_all()))
        self.assertEqual(rp.flat_map(foo).count(), 10)

    def test_glom(self):
        options = get_default_options()
        rp = self.ctx.load("ns1", "test_glom", options=options).put_all(range(5), options=options)
        print(list(rp.glom().get_all()))
        self.assertEqual(get_value(rp.glom()), [(4, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)])])

    def test_join(self):
        options = get_default_options()
        left_rp = self.ctx.load("ns1", "testJoinLeft", options=options).put_all([('a', 1), ('b', 4)], options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testJoinRight", options=options).put_all([('a', 2), ('c', 4)], options={"include_key": True})
        print(list(left_rp.join(right_rp, lambda v1, v2: v1 + v2).get_all()))
        self.assertEqual(get_value(left_rp.join(right_rp, lambda v1, v2: v1 + v2)), [('a', 3)])

    def test_reduce(self):
        from operator import add
        options = get_default_options()
        rp = self.ctx.load("ns1", "testReduce", options=options).put_all(range(20), options=options)
        print(list(rp.reduce(add).get_all()))
        self.assertEqual(get_value(rp.reduce(add)), [(b'result', 190)])

    def test_multi_partition_reduce(self):
        from operator import add
        options = get_default_options()
        rp = self.ctx.load("ns1", "testMultiPartitionReduce", options=options).put_all(range(20), options=options)
        print(list(rp.reduce(add).get_all()))
        self.assertEqual(get_value(rp.reduce(add)), [(b'result', 190)])

    def test_sample(self):
        options = get_default_options()
        rp = self.ctx.load("ns1", "testSample", options=options).put_all(range(100), options=get_default_options())
        print(6 <= rp.sample(0.1, 81).count() <= 14)

    def test_subtract_by_key(self):
        options = get_default_options()
        options['total_partitions'] = 3
        left_rp = self.ctx.load("namespace2020", "testSubtractByKeyLeft2020", options=options).put_all(range(10), options=options)
        right_rp = self.ctx.load("namespace2020", "testSubtractByKeyRight2020", options=options).put_all(range(5), options=options)
        print(list(left_rp.subtract_by_key(right_rp).get_all()))
        left_rp.destroy()
        right_rp.destroy()

    def test_union(self):
        options = get_default_options()
        left_rp = self.ctx.load("ns1", "testUnionLeft", options=options).put_all([1, 2, 3], options=options)

        options['include_key'] = True
        options['total_partitions'] = 3
        right_rp = self.ctx.load("ns1", "testUnionRight", options=options).put_all([(1, 1), (2, 2), (3, 3)])
        print(list(left_rp.union(right_rp, lambda v1, v2: v1 + v2).get_all()))
        left_rp.destroy()
        right_rp.destroy()

        options = get_default_options()

        options['total_partitions'] = 3
        left_rp = self.ctx.load("namespace20200102", "testUnionLeft2020", options=options).put_all([1, 2, 3], options=options)
        print("left:", left_rp)
        options['include_key'] = True
        right_rp = self.ctx.load("namespace20200102", "testUnionRight2020", options=options).put_all([(1, 1), (2, 2), (3, 3)], options=options)
        print("right:", right_rp)
        print("left:", list(left_rp.get_all()))
        print("right:", list(right_rp.get_all()))
        print(list(left_rp.union(right_rp, lambda v1, v2: v1 + v2).get_all()))
        left_rp.destroy()
        right_rp.destroy()

    def test_aggregate(self):
        from operator import add, mul
        options = get_default_options()
        options['total_partitions'] = 3

        rp = self.ctx.load("ns1", "testMultiPartitionAggregate", options=options)
        rp.put_all(range(10), options=options)
        print(list(rp.get_all()))
        print('count:', rp.count())
        print(list(rp.aggregate(zero_value=0, seq_op=add, comb_op=mul).get_all()))
