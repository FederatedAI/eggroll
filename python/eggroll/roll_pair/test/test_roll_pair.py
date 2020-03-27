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
    get_cluster_context, get_standalone_context, get_default_options


def get_value(roll_pair):
    return list(roll_pair.get_all())


class TestRollPairBase(unittest.TestCase):

    def setUp(self):
        self.ctx = get_debug_test_context()

    def tearDown(self) -> None:
        print("stop test session")
        self.ctx.get_session().stop()

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 1}
        opts.update(kwargs)
        return opts

    def assertUnOrderListEqual(self, list1, list2):
        self.assertEqual(sorted(list1), sorted(list2))
    @staticmethod
    def str_generator(include_key=True, row_limit=10, key_suffix_size=0, value_suffix_size=0):
        for i in range(row_limit):
            if include_key:
                yield str(i) + "s"*key_suffix_size, str(i) + "s"*value_suffix_size
            else:
                yield str(i) + "s"*value_suffix_size

    def test_parallelize_include_key(self):
        rp = self.ctx.parallelize(self.str_generator(True),
                                  options=self.store_opts(include_key=True))
        self.assertUnOrderListEqual(self.str_generator(True), rp.get_all())
        #rp.destroy()

    def test_parallelize(self):
        rp = self.ctx.parallelize(self.str_generator(False), options=self.store_opts(include_key=False))
        print(rp)
        print(list(rp.get_all()))
        self.assertUnOrderListEqual(self.str_generator(False), (v for k,v in rp.get_all()))
        #rp.destroy()

    def test_serdes(self):
        rp = self.ctx.load("ns12020","n_serdes", self.store_opts(serdes="EMPTY"))
        rp.put_all((b"a",b"b") for k in range(10))
        print(list(rp.get_all()))
        print(rp.count())

    def test_get(self):
        rp = self.ctx.parallelize(self.str_generator())
        for i in range(10):
            self.assertEqual(str(i), rp.get(str(i)))
        #rp.destroy()

    def test_count(self):
        rp = self.ctx.parallelize(self.str_generator(row_limit=11))
        self.assertEqual(11, rp.count())
        #rp.destroy()

    def test_put_all(self):
        rp = self.ctx.load("ns12020","n1")
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)
        self.assertUnOrderListEqual(data, rp.get_all())
        #rp.destroy()

    def test_cleanup(self):
        rp = self.ctx.load("ns168","n1")
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)

        rp1 = self.ctx.load("ns168","n111")
        rp1.put_all(data)
        self.ctx.cleanup(namespace='ns168', name='*')

    def test_map(self):
        rp = self.ctx.parallelize(self.str_generator())
        rp2 = rp.map(lambda k,v: (k + "_1", v))
        self.assertUnOrderListEqual(((k + "_1", v) for k, v in self.str_generator()), rp2.get_all())
        #rp.destroy()
        #rp2.destroy()

    def test_reduce(self):
        options = self.store_opts()
        #options['total_partitions'] = 10
        rp = self.ctx.parallelize([(i, i) for i in range(1, 7)], options)
        #data = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]
        #rp.put_all(data)
        print(list(rp.get_all()))
        print(rp.count())
        from operator import add
        result = rp.reduce(add)

        print(f'reduce result: {result}')
        self.assertEqual(result, 21)

    def test_reduce_numpy(self):
        import numpy as np
        rp = self.ctx.load('ns12020', 'testNumpyReduce', self.store_opts())
        rp.put('0', np.array([[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 0]]))
        rp.put('1', np.array([[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 1]]))
        rp.put('2', np.array([[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 2]]))
        rp.put('3', np.array([[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 3]]))
        rp.put('4', np.array([[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 4]]))
        rp.put('5', np.array([[ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 5]]))
        #rp.put('6', None)

        result = rp.reduce(lambda x, y: x + y)

        print(result)
        self.assertEqual(result[0][-1], 15)

    def test_aggregate(self):
        from operator import mul, add
        options = self.store_opts()
        #options['total_partitions'] = 10
        data1 = self.ctx.parallelize([(i, i) for i in range(1, 7)], options)
        print(data1.get_partitions())
        h2 = data1.aggregate(zero_value=1, seq_op=mul, comb_op=add)
        print("aggregate result: ", h2)
        self.assertEqual(h2, 25)
        #self.assertEqual(h2, 720)

    def test_join_self(self):
        options = get_default_options()
        left_rp = self.ctx.load("ns12020", "testJoinLeft2020", options=options).put_all([('a', 1), ('b', 4)], options={"include_key": True})
        print(list(left_rp.join(left_rp, lambda v1, v2: v1 + v2).get_all()))
        self.assertEqual(get_value(left_rp.join(left_rp, lambda v1, v2: v1 + v2)), [('a', 2), ('b', 8)])

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
        table = self.ctx.load('ns12020020618', 'test_destroy', options=options)#.put_all(data, options=options)
        print("before destroy:{}".format(list(table.get_all())))
        table.destroy()
        # TODO:1: table which has been destroyed cannot get_all, should raise exception
        #print("after destroy:{}".format(list(table.get_all())))
        self.assertEqual(table.count(), 0)

    def test_destroy_simple(self):
        options = get_default_options()
        options['include_key'] = True
        table = self.ctx.load('ns1', 'test_destroy', options=options)
        table.destroy()

    def test_take(self):
        options = get_default_options()
        options['keys_only'] = True
        options['include_key'] = False
        table = self.ctx.load('ns1', 'test_take', options=options).put_all(range(10), options=options)
        print(table.take(n=3, options=options))
        self.assertEqual(table.take(n=3, options=options), [0, 1, 2])

        options_kv = get_default_options()
        options_kv['keys_only'] = False
        options_kv['include_key'] = False
        table = self.ctx.load('ns1', 'test_take_kv', options=options_kv).put_all(range(10), options=options_kv)
        print(table.take(n=3, options=options_kv))
        self.assertEqual(table.take(n=3, options=options_kv), [(0, 0), (1, 1), (2, 2)])

    def test_first(self):
        options = get_default_options()
        options['keys_only'] = True
        options['include_key'] = False
        table = self.ctx.load('ns1', 'test_take', options=options).put_all(range(10), options=options)
        print(table.first(options=options))
        self.assertEqual(table.first(options=options), 0)

        options_kv = get_default_options()
        options_kv['include_key'] = False
        options_kv['keys_only'] = False
        table = self.ctx.load('ns12020', 'test_take_kv', options=options_kv).put_all(range(10), options=options_kv)
        print(table.first(options=options_kv))
        self.assertEqual(table.first(options=options_kv), (0, 0))

    def test_map_values(self):
        options = get_default_options()
        options['include_key'] = False
        rp = self.ctx.load("ns12020", "test_map_values", options=options).put_all(range(10), options=options)
        res = rp.map_values(lambda v: str(v) + 'map_values')
        print(list(res.get_all()))
        self.assertEqual(get_value(res), [(0, '0map_values'), (1, '1map_values'), (2, '2map_values'), (3, '3map_values'),
                                          (4, '4map_values'), (5, '5map_values'), (6, '6map_values'), (7, '7map_values'),
                                          (8, '8map_values'), (9, '9map_values')])

    def test_map_partitions(self):
        options = get_default_options()
        options['total_partitions'] = 12
        data = [(str(i), i) for i in range(10)]
        rp = self.ctx.load("ns1", "test_map_partitions", options=options).put_all(data, options={"include_key": True})
        def func(iter):
            ret = []
            for k, v in iter:
                ret.append((f"{k}_{v}_0", v ** 2))
                ret.append((f"{k}_{v}_1", v ** 3))
            return ret
        table = rp.map_partitions(func)
        self.assertEqual(table.get("6_6_0"), 36)
        self.assertEqual(table.get("0_0_1"), 0)
        self.assertEqual(table.get("1_1_0"), 1)
        self.assertEqual(sorted(table.get_all(), key=lambda x: x[0]), [('0_0_0', 0), ('0_0_1', 0), ('1_1_0', 1), ('1_1_1', 1), ('2_2_0', 4), ('2_2_1', 8),
                                            ('3_3_0', 9), ('3_3_1', 27), ('4_4_0', 16), ('4_4_1', 64), ('5_5_0', 25),
                                            ('5_5_1', 125), ('6_6_0', 36), ('6_6_1', 216), ('7_7_0', 49), ('7_7_1', 343),
                                            ('8_8_0', 64), ('8_8_1', 512), ('9_9_0', 81), ('9_9_1', 729)])

    def test_collapse_partitions(self):
        options = get_default_options()
        options['include_key'] = False
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
        options['include_key'] = False
        rp = self.ctx.load("ns1", "test_filter", options=options).put_all(range(5), options=options)
        print(list(rp.filter(lambda k, v: v % 2 != 0).get_all()))
        self.assertEqual(get_value(rp.filter(lambda k, v: v % 2 != 0)), [(1, 1), (3, 3)])

    def test_flatMap(self):
        options = get_default_options()
        options['include_key'] = False
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
        options['include_key'] = False
        rp = self.ctx.load("ns1", "test_glom", options=options).put_all(range(5), options=options)
        print(list(rp.glom().get_all()))
        self.assertEqual(get_value(rp.glom()), [(4, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)])])

    def test_join(self):
        options = get_default_options()
        left_rp = self.ctx.load("ns1", "testJoinLeft", options=options).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0)], options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testJoinRight", options=options).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)], options={"include_key": True})
        print(list(left_rp.join(right_rp, lambda v1, v2: v1 + v2).get_all()))
        self.assertEqual(get_value(left_rp.join(right_rp, lambda v1, v2: v1 + v2)), [('a', 3), ('d', 7)])
        self.assertEqual(get_value(right_rp.join(left_rp, lambda v1, v2: v1 + v2)), [('a', 3), ('d', 7)])

    def test_sample(self):
        options = get_default_options()
        options['include_key'] = False
        rp = self.ctx.load("ns1", "testSample", options=options).put_all(range(100), options=options)
        self.assertEqual(6 <= rp.sample(0.1, 81).count() <= 14, True)

    def test_subtract_by_key(self):
        options = get_default_options()
        options['total_partitions'] = 1
        options['include_key'] = False
        left_rp = self.ctx.load("namespace20201", "testSubtractByKeyLeft202013", options=options).put_all(range(10), options=options)
        right_rp = self.ctx.load("namespace2020131", "testSubtractByKeyRight202013", options=options).put_all(range(5), options=options)
        self.assertEqual(list(left_rp.subtract_by_key(right_rp).get_all()), [(5, 5), (6, 6), (7, 7), (8, 8), (9, 9)])

    @staticmethod
    def gen_data(self):
        ret = []
        for i in range(1, 2000000):
            ret.append(i)
        return ret

    @staticmethod
    def gen_kv(self):
        for i in range(1, 2000000):
            yield [i, i]

    def test_union(self):
        options = get_default_options()
        options['include_key'] = False
        left_rp = self.ctx.load("ns1202010", "testUnionLeft2020", options=options).put_all([1, 2, 3], options=options)
        print(left_rp)
        options['include_key'] = True
        options['total_partitions'] = 1
        right_rp = self.ctx.load("ns12020101", "testUnionRight2020", options=options).put_all([(1, 1), (2, 2), (3, 3)])
        print(right_rp)
        print(list(left_rp.union(right_rp, lambda v1, v2: v1 + v2).get_all()))

        options = get_default_options()

        options['total_partitions'] = 1
        options['include_key'] = False
        left_rp = self.ctx.load("namespace20200110", "testUnionLeft2020", options=options).put_all([1, 2, 3], options=options)
        print("left:", left_rp)
        options['include_key'] = True
        right_rp = self.ctx.load("namespace20200110", "testUnionRight2020", options=options).put_all([(1, 1), (2, 2), (3, 3)], options=options)
        print("right:", right_rp)
        print("left:", list(left_rp.get_all()))
        print("right:", list(right_rp.get_all()))
        print(list(left_rp.union(right_rp, lambda v1, v2: v1 + v2).get_all()))


class TestRollPairMultiPartition(TestRollPairBase):
    def setUp(self):
        self.ctx = get_debug_test_context()

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 3}
        opts.update(kwargs)
        return opts

    @staticmethod
    def str_generator(include_key=True, row_limit=100, key_suffix_size=0, value_suffix_size=0):
        return TestRollPairBase.str_generator(include_key, row_limit, key_suffix_size, value_suffix_size)

    def test_put_all(self):
        st_opts = self.store_opts(include_key=True)
        rp = self.ctx.load("test_roll_pair", "TestRollPairMultiPartition", options=self.store_opts())
        row_limit = 3
        rp.put_all(self.str_generator(row_limit=row_limit))

        self.assertUnOrderListEqual(self.str_generator(include_key=True, row_limit=row_limit), rp.get_all())
        self.assertEqual(st_opts["total_partitions"], rp.get_partitions())
        #rp.destroy()

    def test_count(self):
        st_opts = self.store_opts(include_key=True)
        rp = self.ctx.load("test_roll_pair", "TestRollPairMultiPartition", options=self.store_opts())
        count = rp.count()
        print(count)
        self.assertEqual(count, 10000)

    def test_reduce_numpy(self):
        super().test_reduce_numpy()

    def test_parallelize_include_key(self):
        st_opts = self.store_opts(include_key=True)
        rp = self.ctx.parallelize(self.str_generator(True),st_opts)
        self.assertUnOrderListEqual(self.str_generator(True), rp.get_all())
        self.assertEqual(st_opts["total_partitions"], rp.get_partitions())
        rp.destroy()

    def test_count(self):
        super().test_count()

    def test_reduce(self):
        super().test_reduce()

    def test_aggregate(self):
        from operator import mul, add
        data1 = self.ctx.parallelize([(i, i) for i in range(1, 7)], self.store_opts())
        print(data1.get_partitions())
        h2 = data1.aggregate(zero_value=1, seq_op=mul, comb_op=add)
        print(f"aggregate result: {h2}")

        self.assertEqual(h2, 32)


class TestRollPairStandalone(TestRollPairBase):
    ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.ctx = get_standalone_context()

    def setUp(self):
        pass

    def tearDown(self) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        cls.ctx.get_session().stop()


class TestRollPairClusterEverySession(TestRollPairBase):
    def setUp(self):
        self.ctx = get_cluster_context()

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 10}
        opts.update(kwargs)
        return opts

    def test_get_and_stop_and_kill_session(self):
        session = self.ctx.get_session()
        id = session.get_session_id()

        session.stop()

        from eggroll.core.session import ErSession
        dead_session = ErSession(id)
        dead_session.stop()

        dead_session = ErSession(id)
        dead_session.kill()

    def tearDown(self) -> None:
        self.ctx.get_session().stop()


class TestRollPairCluster(TestRollPairBase):
    ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "10"}
        #opts = {}
        cls.ctx = get_cluster_context(options=opts)

    def setUp(self):
        pass

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 10}
        opts.update(kwargs)
        return opts

    def test_aggregate(self):
        from operator import mul, add
        data1 = self.ctx.parallelize([(i, i) for i in range(1, 7)], self.store_opts())
        print(data1.get_partitions())
        h2 = data1.aggregate(zero_value=1, seq_op=mul, comb_op=add)
        print("aggregate result: ", h2)

        # note that if there is no data in a partition then the zero value will be sent, thus 21 + 4 * 1 = 25
        self.assertEqual(h2, 25)

    def test_map_values(self):
        super().test_map_values()

    def test_reduce(self):
        rp = self.ctx.parallelize([(i, i) for i in range(1, 7)], self.store_opts())

        #data = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]
        #rp.put_all(data)
        print("all: ", list(rp.get_all()))
        print("count: ", rp.count())
        from operator import add
        result = rp.reduce(add)

        print(f'reduce result: {result}')
        self.assertEqual(result, 21)

    def test_empty(self):
        pass

    def tearDown(self) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        cls.ctx.get_session().stop()
