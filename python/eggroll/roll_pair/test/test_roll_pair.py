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
        rp.destroy()

    def test_parallelize(self):
        rp = self.ctx.parallelize(self.str_generator(False), options=self.store_opts(include_key=False))
        self.assertUnOrderListEqual(self.str_generator(False), (v for k,v in rp.get_all()))
        rp.destroy()

    def test_get(self):
        rp = self.ctx.parallelize(self.str_generator())
        for i in range(10):
            self.assertEqual(str(i), rp.get(str(i)))
        rp.destroy()

    def test_map(self):
        rp = self.ctx.parallelize(self.str_generator())
        rp2 = rp.map(lambda k,v: (k + "_1", v))
        self.assertUnOrderListEqual(((k + "_1", v) for k, v in self.str_generator()), rp2.get_all())
        rp.destroy()
        rp2.destroy()

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

class TestRollPairCluster(TestRollPairBase):
    def setUp(self):
        self.ctx = get_cluster_context()

