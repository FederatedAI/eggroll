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


class TestRollPairBase(unittest.TestCase):

    def setUp(self):
        self.ctx = get_debug_test_context()
        self.row_limit = 10
        self.key_suffix_size = 0
        self.value_suffix_size = 0

    @staticmethod
    def store_opts(**kwargs):
        opts= {'total_partitions':1}
        opts.update(kwargs)
        return opts

    def assertUnOrderListEqual(self, list1, list2):
        self.assertEqual(sorted(list1), sorted(list2))

    def str_generator(self, include_key=True):
        for i in range(self.row_limit):
            if include_key:
                yield str(i) + "s"*self.key_suffix_size, str(i) + "s"*self.value_suffix_size
            else:
                yield str(i) + "s"*self.value_suffix_size

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

    def test_multi_partition_map(self):
        options = get_default_options()
        options['total_partitions'] = 3
        options['include_key'] = False
        rp = self.ctx.load("ns1", "testMultiPartitionsMap", options=options).put_all(range(100), options=options)

        result = rp.map(lambda k, v: (k + 1, v))
        print(result.count())

class TestRollPairMultiPartition(TestRollPairBase):
    def setUp(self):
        self.ctx = get_debug_test_context()
        self.row_limit = 10
        self.key_suffix_size = 0
        self.value_suffix_size = 0

    @staticmethod
    def store_opts(**kwargs):
        opts= {'total_partitions':3}
        opts.update(kwargs)
        return opts

    def test_parallelize_include_key(self):
        st_opts = self.store_opts(include_key=True)
        rp = self.ctx.parallelize(self.str_generator(True),st_opts)
        self.assertUnOrderListEqual(self.str_generator(True), rp.get_all())
        self.assertEqual(st_opts["total_partitions"], rp.get_partitions())
        rp.destroy()

class TestRollPairCluster(TestRollPairBase):
    def setUp(self):
        self.ctx = get_cluster_context()
        self.row_limit = 10
        self.key_suffix_size = 0
        self.value_suffix_size = 0

