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

    @staticmethod
    def store_opts(**kwargs):
        opts= {'total_partitions':1}
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
        rp = self.ctx.load("ns1","n_serdes", self.store_opts(serdes="EMPTY"))
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
        rp = self.ctx.load("ns1","n1")
        data = [("k1","v1"),("k2","v2"),("k3","v3"),("k4","v4"),("k5","v5"),("k6","v6")]
        rp.put_all(data)
        self.assertUnOrderListEqual(data, rp.get_all())
        #rp.destroy()

    def test_map(self):
        rp = self.ctx.parallelize(self.str_generator())
        rp2 = rp.map(lambda k,v: (k + "_1", v))
        self.assertUnOrderListEqual(((k + "_1", v) for k, v in self.str_generator()), rp2.get_all())
        #rp.destroy()
        #rp2.destroy()

    def test_reduce(self):
        rp = self.ctx.load("ns1","n_serdes", self.store_opts(serdes="EMPTY"))
        rp.put_all((b'1', b'2') for k in range(10))
        print(list(rp.get_all()))
        print(rp.count())
        from operator import add
        print(list(rp.reduce(add).get_all()))

    def test_join_self(self):
        options = get_default_options()
        left_rp = self.ctx.load("ns1", "testJoinLeft2020", options=options).put_all([('a', 1), ('b', 4)], options={"include_key": True})
        print(list(left_rp.join(left_rp, lambda v1, v2: v1 + v2).get_all()))
        self.assertEqual(get_value(left_rp.join(left_rp, lambda v1, v2: v1 + v2)), [('a', 2), ('b', 8)])

class TestRollPairMultiPartition(TestRollPairBase):
    def setUp(self):
        self.ctx = get_debug_test_context()

    @staticmethod
    def store_opts(**kwargs):
        opts= {'total_partitions':3}
        opts.update(kwargs)
        return opts

    @staticmethod
    def str_generator(include_key=True, row_limit=100, key_suffix_size=0, value_suffix_size=0):
        return TestRollPairBase.str_generator(include_key, row_limit, key_suffix_size, value_suffix_size)

    def test_put_all(self):
        st_opts = self.store_opts(include_key=True)
        rp = self.ctx.load("test_roll_pair","TestRollPairMultiPartition", options=self.store_opts())
        rp.put_all(self.str_generator())
        self.assertUnOrderListEqual(self.str_generator(include_key=True), rp.get_all())
        self.assertEqual(st_opts["total_partitions"], rp.get_partitions())
        rp.destroy()

    def test_parallelize_include_key(self):
        st_opts = self.store_opts(include_key=True)
        rp = self.ctx.parallelize(self.str_generator(True),st_opts)
        self.assertUnOrderListEqual(self.str_generator(True), rp.get_all())
        self.assertEqual(st_opts["total_partitions"], rp.get_partitions())
        rp.destroy()


class TestRollPairStandalone(TestRollPairBase):
    ctx = None

    def setUp(self):
        pass

    @classmethod
    def setUpClass(cls) -> None:
        cls.ctx = get_standalone_context()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.ctx.get_session().stop()


class TestRollPairCluster(TestRollPairBase):
    def setUp(self):
        self.ctx = get_cluster_context()

