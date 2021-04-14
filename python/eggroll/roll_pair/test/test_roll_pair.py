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

from eggroll.core.conf_keys import CoreConfKeys
from eggroll.core.constants import StoreTypes, SerdesTypes
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.utils import time_now
from eggroll.roll_pair import create_adapter
from eggroll.roll_pair.test.roll_pair_test_assets import get_debug_test_context, \
    get_cluster_context, get_standalone_context, get_default_options


def get_value(roll_pair):
    return list(sorted(roll_pair.get_all(), key=lambda x: x[0]))


class TestRollPairBase(unittest.TestCase):

    def setUp(self):
        self.ctx = get_debug_test_context()

    def tearDown(self) -> None:
        print("stop test session")
        # self.ctx.get_session().stop()

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 1, "create_if_missing": True}
        opts.update(create_if_missing=True)
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

    def test_parallelize(self):
        rp = self.ctx.parallelize(self.str_generator(False), options=self.store_opts(include_key=False))
        print(rp)
        print(list(rp.get_all()))
        self.assertUnOrderListEqual(self.str_generator(False), (v for k,v in rp.get_all()))

    def test_load_destroy_load(self):
        options = {'total_partitions': 16}
        rp = self.ctx.load('test_ns', 'test_name16', options=self.store_opts(total_partitions=16))
        rp.destroy()
        rp = self.ctx.load('test_ns', 'test_name16', self.store_opts(total_partitions=16))

    def test_session_meta(self):
        table = self.ctx.load(self.ctx.session_id, 'er_session_meta', options={'store_type': StoreTypes.ROLLPAIR_CACHE})
        def get_eggs(task):
            _input = task._inputs[0]
            with create_adapter(_input) as input_adapter:
                eggs = input_adapter.get('eggs')
                return eggs

        print(table.with_stores(get_eggs))

    def test_ldl(self):
        from eggroll.core.session import session_init
        from eggroll.roll_pair.roll_pair import RollPairContext
        import uuid
        options = dict()
        print(111)
        print(222)
        table = self.ctx.load(name='fate_flow_detect_table_name', namespace='fate_flow_detect_table_namespace',
                              options=self.store_opts(total_partitions=16))
        table.put_all([("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")])
        print(table.count())
        print(333)
        table.destroy()
        table = self.ctx.load(name='fate_flow_detect_table_name', namespace='fate_flow_detect_table_namespace',
                              options=self.store_opts(total_partitions=16))

    def test_empty_serdes(self):
        rp = self.ctx.load('empty_serdes1', 'empty_serdes_ns', options={'serdes': SerdesTypes.EMPTY, 'create_if_missing': True, 'total_partitions': 2})
        rp.put(b'k1', b'v1')
        rp.put(b'k2', b'v2')

        print(rp.count())
        elements = list(rp.get_all())
        print(elements)

    def test_sc(self):
        options_left = get_default_options()
        options_right = get_default_options()
        options_left['total_partitions'] = 10
        options_right['total_partitions'] = 5
        left_rp = self.ctx.load(namespace="ns1", name="testSubtractLeft_10p_8", options=options_left).put_all([('a', 1), ('b', 4), ('d', 6),
                                                                                                               ('e', 0), ('f', 3),],
                                                                                                              options={"include_key": True})
        right_rp = self.ctx.load(namespace="ns1", name="testSubtractRight_5p_8", options=options_right).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                                                options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('111', get_value(left_rp.subtract_by_key(right_rp)))
        print('222', left_rp.subtract_by_key(right_rp).get_partitions())

    def test_parallelize_map_values(self):
        rp = self.ctx.parallelize(self.str_generator(False), options=self.store_opts(include_key=False))
        print(rp)
        print(list(rp.get_all()))
        self.assertUnOrderListEqual(self.str_generator(False), (v for k,v in rp.get_all()))
        rp.map_values(lambda v:v)

    def test_serdes(self):
        rp = self.ctx.load("ns12020","n_serdes", self.store_opts(serdes="EMPTY"))
        rp.put_all((b"a",b"b") for k in range(10))
        print(list(rp.get_all()))
        print(rp.count())

    def test_put(self):
        rp = self.ctx.load('ns12020', f'test_put_{time_now()}', options=self.store_opts())
        object = b'1' * 10
        rp.put(b'k1', object)
        rp.destroy()

    def test_get(self):
        rp = self.ctx.parallelize(self.str_generator())
        for i in range(10):
            self.assertEqual(str(i), rp.get(str(i)))

    def test_put_get(self):
        rp = self.ctx.load('ns12020', f'test_put_get_{time_now()}', options=self.store_opts())
        length = (2 << 10) - 10
        k = b'k'
        v = b'1' * length
        rp.put(k, v)
        v1 = rp.get(k)
        print(f'length: {len(v1)}')
        self.assertEqual(len(v1), length)
        self.assertEqual(v, v1)

    def test_count(self):
        rp = self.ctx.parallelize(self.str_generator(row_limit=11))
        self.assertEqual(11, rp.count())

    def test_put_all(self):
        rp = self.ctx.load("ns12020","n1", options=self.store_opts())
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)
        self.assertUnOrderListEqual(data, rp.get_all())

    def test_put_all_multi_thread(self):
        executor_pool_type = CoreConfKeys.EGGROLL_CORE_DEFAULT_EXECUTOR_POOL.get()
        exe = create_executor_pool(canonical_name=executor_pool_type, max_workers=2)
        exe.submit(self.test_put_all)

    def test_cleanup(self):
        rp = self.ctx.load("ns168","n1", options=self.store_opts())
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)

        rp1 = self.ctx.load("ns168","n111", options=self.store_opts())
        rp1.put_all(data)
        self.ctx.cleanup(namespace='ns168', name='n11*')

    def test_cleanup_namespace(self):
        namespace = 'ns180'
        rp = self.ctx.load(namespace,"n1", options=self.store_opts())
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)

        rp1 = self.ctx.load(namespace,"n111", options=self.store_opts())
        rp1.put_all(data)
        rp2 = self.ctx.parallelize(data, options={'namespace': namespace})
        self.ctx.cleanup(namespace=namespace, name='*')

    def test_cleanup_namespace_specified_store_type(self):
        namespace = 'ns181'
        rp = self.ctx.load(namespace,"n1", options=self.store_opts())
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)

        rp1 = self.ctx.parallelize(data, options=self.store_opts(namespace=namespace))
        self.ctx.cleanup(namespace=namespace,
                         name='*',
                         options={'store_type': StoreTypes.ROLLPAIR_IN_MEMORY})

    def test_map(self):
        rp = self.ctx.parallelize(self.str_generator())
        rp2 = rp.map(lambda k, v: (k + "_1", v))
        print(list(rp2.get_all()))
        self.assertUnOrderListEqual(((k + "_1", v) for k, v in self.str_generator()), rp2.get_all())

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
        #self.assertEqual(h2, 25)
        self.assertEqual(h2, 720)

    def test_join_self(self):
        options = get_default_options()
        left_rp = self.ctx.load("ns12020", "testJoinLeft2020", options=self.store_opts()).put_all([('a', 1), ('b', 4)],
                                                                                        options=self.store_opts(include_key=True))
        print(list(left_rp.join(left_rp, lambda v1, v2: v1 + v2).get_all()))
        self.assertEqual(get_value(left_rp.join(left_rp, lambda v1, v2: v1 + v2)), [('a', 2), ('b', 8)])

    def test_delete(self):
        options = get_default_options()
        options['include_key'] = True
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns1', 'test_delete_one', options=self.store_opts(include_key=True)).\
            put_all(data, options=self.store_opts(include_key=True))
        print("before delete:{}".format(list(table.get_all())))
        table.delete("k1")
        print("after delete:{}".format(list(table.get_all())))
        self.assertEqual(get_value(table), ([("k2", "v2"), ("k3", "v3"), ("k4", "v4")]))

    def test_destroy(self):
        options = get_default_options()
        options['include_key'] = True

        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")]
        table = self.ctx.load('ns12020020618', 'test_destroy', options=self.store_opts(include_key=True))\
            .put_all(data, options=self.store_opts(include_key=True))
        print("before destroy:{}".format(list(table.get_all())))
        table.destroy()
        # TODO:1: table which has been destroyed cannot get_all, should raise exception
        self.assertEqual(table.count(), 0)

    def test_destroy_simple(self):
        options = get_default_options()
        options['include_key'] = True
        table = self.ctx.load('ns1', 'test_destroy', options=self.store_opts(include_key=True))
        table.destroy()

    def test_take(self):
        alist=[("something1",  1), ("something3", 2), ("something2", 2), ("something10", 3), ("something125", 4), ("something5", 5),
               ("something16", 6), ("something0", 6), ("something4", 6)]
        blist=[("something1",  1), ("something3", 2), ("something2", 2), ("something1.34", 3), ("something1.0", 3), ("something1.25", 4), ("something1.105", 5),
               ("something0.105", 6), ("something0.104", 6)]
        clist = [('1112', '2'), ('35', '5'), ('18', '8'), ('0', '0'), ('23', '3'), ('6', '6'), ('9', '9'), ('1', '1'), ('4', '4'), ('7', '7')]
        dlist = [('1112.1', '2'), ('35.2', '5'), ('18.3', '8'), ('0', '0'), ('23.9', '3'), ('35.1', '6'), ('18.2', '9'), ('1', '1'), ('4', '4'), ('23.6', '7')]
        elist=[("1something",  1), ("3tomething", 2), ("3something", 2), ("2something", 2), ("1.34something", 3), ("1.0something", 3),
               ("1.25something", 4), ("1.105something", 5),
               ("0.105something", 6)]

        all_list = [alist, blist, clist, dlist, elist]

        for lst in all_list:
            options = get_default_options()
            options['keys_only'] = True
            options['include_key'] = True
            options['total_partitions'] = 3
            table = self.ctx.parallelize(lst, options=options)
            print(f'get_all:{list(table.get_all())}')
            print('start take')
            print(table.take(n=6, options=options))
            self.assertEqual(table.take(n=3, options=options),
                             [item[0] for item in list(table.get_all())[:3]])

            options_kv = get_default_options()
            options_kv['keys_only'] = False
            options_kv['include_key'] = True
            options_kv['total_partitions'] = 3
            table = self.ctx.parallelize(lst, options=options_kv)
            print(f'get_all:{list(table.get_all())}')
            print('start take')
            print(table.take(n=6, options=options_kv))
            self.assertEqual(table.take(n=3, options=options_kv), list(table.get_all())[:3])

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

    def test_map_values_many(self):
        options = get_default_options()
        options['include_key'] = False
        rp = self.ctx.load("ns12020", "test_map_values", options=options).put_all(range(10), options=options)

        for i in range(100):
            rp.map_values(lambda v:v)

    def test_map_partitions(self):
        options = get_default_options()
        options['total_partitions'] = 10
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

    def test_map_partitions_with_reduce(self):
        options = get_default_options()
        options['total_partitions'] = 3
        data = [('a', 1), ('b', 2), ('c', 10), ('d', 4), ('e', 5), ('f', 20), ('g', 6), ('h', 7), ('i', 30), ('j', 66)]
        rp = self.ctx.load("ns1", "test_map_partitions_with_reduce_3par",
                           options=options).put_all(data, options={"include_key": True})

        def func(iter):
            ret = []
            for k, v in iter:
                print(f'k:{k}, v:{v}')
                if k in ('a', 'b', 'c'):
                    k = 'A'
                elif k in ('d', 'e', 'f'):
                    k = 'B'
                elif k in ('g', 'h', 'i'):
                    k = 'C'
                ret.append((f"{k}_0", v))
                ret.append((f"{k}_1", v+1))
            print(f'lambda ret:{ret}')
            return ret
        from operator import add
        table = rp.map_partitions(func, reduce_op=add)
        print(f"res:{sorted(list(table.get_all()), key=lambda x: x[0])}")
        self.assertEqual(sorted(list(table.get_all()), key=lambda x: x[0]),
                         [('A_0', 13), ('A_1', 16), ('B_0', 29), ('B_1', 32),
                          ('C_0', 43), ('C_1', 46), ('j_0', 66), ('j_1', 67)])
        self.assertEqual(table.get('A_0'), 13)
        self.assertEqual(table.get('A_1'), 16)
        self.assertEqual(table.get('B_0'), 29)
        self.assertEqual(table.get('B_1'), 32)
        self.assertEqual(table.get('C_0'), 43)
        self.assertEqual(table.get('C_1'), 46)
        self.assertEqual(table.get('j_0'), 66)
        self.assertEqual(table.get('j_1'), 67)

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
        options['total_partitions'] = 3
        rp = self.ctx.load("ns1", "test_flat_map_3p", options=options).put_all(range(5), options=options)
        import random

        def foo(k, v):
            result = []
            r = random.randint(10000, 99999)
            for i in range(0, k):
                result.append((k + r + i, v + r + i + 1))
            return result
        res = rp.flat_map(foo)
        print(list(res.get_all()))
        print(f"get value:{res.get(res.first(options={'keys_only': True}))} of key:{res.first(options={'keys_only': True})}")
        self.assertEqual(res.first(options={'keys_only': False})[1], res.get(res.first(options={'keys_only': True})))
        self.assertEqual(res.count(), 10)

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

    def test_join_diff_partitions(self):
        options_left = get_default_options()
        options_right = get_default_options()
        options_left['total_partitions'] = 10
        options_right['total_partitions'] = 5
        left_rp = self.ctx.load("ns1", "testJoinLeft_10p_6", options=options_left).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0), ('f', 3), ('g', 12), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)],
                                                                                    options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testJoinRight_5p_6", options=options_right).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                     options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('111', get_value(left_rp.join(right_rp, lambda v1, v2: v1 + v2)))
        print('222', get_value(right_rp.join(left_rp, lambda v1, v2: v1 + v2)))
        self.assertEqual(get_value(left_rp.join(right_rp, lambda v1, v2: v1 + v2)), [('a', 3), ('d', 7), ('f', 3), ('g', 13)])
        self.assertEqual(get_value(right_rp.join(left_rp, lambda v1, v2: v1 + v2)), [('a', 3), ('d', 7), ('f', 3), ('g', 13)])

        right_rp = self.ctx.load("ns1", "testJoinRight_10p_7", options=options_right).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0), ('f', 3), ('g', 12), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)],
                                                                                           options={"include_key": True})
        left_rp = self.ctx.load("ns1", "testJoinLeft_5p_7", options=options_left).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                             options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('333', get_value(left_rp.join(right_rp, lambda v1, v2: v1 + v2)))
        self.assertEqual(get_value(left_rp.join(right_rp, lambda v1, v2: v1 + v2)), [('a', 3), ('d', 7), ('f', 3), ('g', 13)])

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
        print(list(left_rp.subtract_by_key(right_rp).get_all()))

    def test_subtract_by_key_second(self):
        options_left = get_default_options()
        options_right = get_default_options()
        options_left['total_partitions'] = 1
        options_right['total_partitions'] = 1
        left_rp = self.ctx.load("ns1", "testSubtractLeft_10p_3", options=options_left).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)], options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testSubtractRight_5p_3", options=options_right).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0)],
                                                                                                 options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('111', get_value(left_rp.subtract_by_key(right_rp)))
        self.assertEqual([('c', 4), ('f', 0), ('g', 1)], get_value(left_rp.subtract_by_key(right_rp)))

    def test_subtract_diff_partitions(self):
        options_left = get_default_options()
        options_right = get_default_options()
        options_left['total_partitions'] = 10
        options_right['total_partitions'] = 5
        left_rp = self.ctx.load("ns1", "testSubtractLeft_10p_7", options=options_left).put_all([('a', 1), ('b', 4), ('d', 6),
                                                                                                ('e', 0), ('f', 3), ('g', 12), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)],
                                                                                           options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testSubtractRight_5p_7", options=options_right).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                             options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('111', get_value(left_rp.subtract_by_key(right_rp)))
        print('222', get_value(right_rp.subtract_by_key(left_rp)))
        rs = get_value(left_rp.subtract_by_key(right_rp))
        print('rs:', rs)
        self.assertEqual([('b', 4), ('e', 0), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)], rs)
        self.assertEqual(get_value(right_rp.subtract_by_key(left_rp)), [('c', 4)])

        right_rp = self.ctx.load("ns1", "testSubtractRight_10p_7", options=options_right).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0), ('f', 3), ('g', 12), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)],
                                                                                              options={"include_key": True})
        left_rp = self.ctx.load("ns1", "testSubtractLeft_5p_7", options=options_left).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                          options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('333', get_value(left_rp.subtract_by_key(right_rp)))
        self.assertEqual(get_value(left_rp.subtract_by_key(right_rp)), [('c', 4)])

    def test_save_as_more_partition(self):
        rp = self.ctx.parallelize(range(10), options={'include_key': False})
        import time
        sa = rp.save_as(f'test_name_{time.monotonic()}', 'test_ns', 2)
        self.assertEqual(sa.get_partitions(), 2)
        self.assertUnOrderListEqual(list(rp.get_all()), list(sa.get_all()))

    def test_save_as_less_partition(self):
        rp = self.ctx.parallelize(range(10), options={'include_key': False, 'total_partitions': 10})
        import time
        sa = rp.save_as(f'test_name_{time.monotonic()}', 'test_ns', 2)
        self.assertEqual(sa.get_partitions(), 2)
        self.assertUnOrderListEqual(list(rp.get_all()), list(sa.get_all()))

    def test_save_as_equal_partition(self):
        rp = self.ctx.parallelize(range(10), options={'include_key': False, 'total_partitions': 2})
        import time
        sa = rp.save_as(f'test_name_{time.monotonic()}', 'test_ns', 2)
        self.assertEqual(sa.get_partitions(), 2)
        self.assertUnOrderListEqual(list(rp.get_all()), list(sa.get_all()))

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

    def test_union_diff_partitions(self):
        options_left = get_default_options()
        options_right = get_default_options()
        options_left['total_partitions'] = 10
        options_right['total_partitions'] = 5
        left_rp = self.ctx.load("ns1", "testUniontLeft_10p_6", options=options_left).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0), ('f', 3), ('g', 12), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)],
                                                                                               options={"include_key": True})
        right_rp = self.ctx.load("ns1", "testUniontRight_5p_6", options=options_right).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                                 options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('111', get_value(left_rp.union(right_rp, lambda v1, v2: v1 + v2)))
        print('222', get_value(right_rp.union(left_rp, lambda v1, v2: v1 + v2)))
        self.assertEqual(get_value(left_rp.union(right_rp, lambda v1, v2: v1 + v2)),
                         [('a', 3), ('b', 4), ('c', 4), ('d', 7), ('e', 0), ('f', 3), ('g', 13), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)])
        self.assertEqual(get_value(right_rp.union(left_rp, lambda v1, v2: v1 + v2)),
                         [('a', 3), ('b', 4), ('c', 4), ('d', 7), ('e', 0), ('f', 3), ('g', 13), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)])

        right_rp = self.ctx.load("ns1", "testUniontRight_10p_7", options=options_right).put_all([('a', 1), ('b', 4), ('d', 6), ('e', 0), ('f', 3), ('g', 12), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)],
                                                                                                  options={"include_key": True})
        left_rp = self.ctx.load("ns1", "testUniontLeft_5p_7", options=options_left).put_all([('a', 2), ('c', 4), ('d', 1), ('f', 0), ('g', 1)],
                                                                                              options={"include_key": True})
        print(f'left:{get_value(left_rp)}, right:{get_value(right_rp)}')
        print('333', get_value(left_rp.union(right_rp, lambda v1, v2: v1 + v2)))
        self.assertEqual(get_value(left_rp.union(right_rp, lambda v1, v2: v1 + v2)),
                         [('a', 3), ('b', 4), ('c', 4), ('d', 7), ('e', 0), ('f', 3), ('g', 13), ('h', 13), ('i', 14), ('j', 15), ('k', 16), ('l', 17)])


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
        opts = {"eggroll.session.processors.per.node": "3"}
        #opts = {}
        cls.ctx = get_cluster_context(options=opts)

    def setUp(self):
        pass

    def test_ldl(self):
        super().test_ldl()

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 10, "create_if_missing": True}
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

    def test_save_as_equal_partition(self):
        super().test_save_as_equal_partition()

    def test_save_as_less_partition(self):
        super().test_save_as_less_partition()

    def test_save_as_more_partition(self):
        super().test_save_as_more_partition()

    def test_join_diff_partitions(self):
        super().test_join_diff_partitions()

    def test_empty(self):
        pass

    def tearDown(self) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        cls.ctx.get_session().stop()


if __name__ == '__main__':
    #suite = unittest.TestSuite()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestRollPairBase)
    runner = unittest.TextTestRunner()
    runner.run(suite)
