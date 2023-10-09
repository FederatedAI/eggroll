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
#
#

import time
import unittest


def kv_generator(limit, data_size):
    s = 'a' * data_size

    for i in range(limit):
        yield i, s


class TestPerformance(unittest.TestCase):
    namespace = "test_performance"
    name = "4G"
    name_r = name + "_r"
    partition = 60
    table = None

    @classmethod
    def setUpClass(cls) -> None:

        # todo: get context
        eggroll.init('test_performance', 1)
        cls.table = eggroll.table(cls.name, cls.namespace, partition=cls.partition)
        cls.table_r = eggroll.table(cls.name_r, cls.namespace, partition=cls.partition)

    @classmethod
    def tearDownClass(cls) -> None:
        # todo: stop session
        eggroll.stop()

    def setUp(self) -> None:
        self.start_time = time.time()

    def tearDown(self) -> None:
        t = time.time() - self.start_time
        print(f"time used: {t}")

    def test_put_all(self):
        self.table.destroy()
        self.table = eggroll.table(cls.name, cls.namespace, partition=cls.partition)
        self.table.put_all(kv_generator(1024 * 1024, 4000))
        print(f"put all count: {self.table.count()}")

    def test_map_values(self):
        b = self.table.mapValues(lambda v : v[1:] + v[0])
        print(f"map_values count: {b.count()}")

    def test_map(self):
        b = self.table.map(lambda k, v : k + 100, v[1:] + v[0])
        print(f"map count: {b.count()}")

    def test_reduce(self):
        b = self.table.reduce(lambda l, r : l)
        print(f"reduce result: {b}")

    def prepare_join(self):
        self.table_r.destroy()
        self.table_r = eggroll.table(self.name_r, self.namespace, partition=self.partition)
        self.table.put_all(kv_generator(1024 * 1024, 4000))
        print(f"prepare join result: {self.table_r.count()}")

    def test_join(self):
        b = self.table.join(self.table_r, lambda v1, v2 : v1[1000:] + v2[0:1000])
        print(f"join count: {b.count()}")


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPerformance)
    unittest.TextTestRunner(verbosity=0).run(suite)

