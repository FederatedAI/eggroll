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

from eggroll.core.test.session_test_asset import get_debug_test_context, \
    get_cluster_context


class TestSessionBase(unittest.TestCase):
    session = None

    def setUp(self) -> None:
        self.session = get_debug_test_context()

    def test_empty(self):
        pass

    def test_kill_all(self):
        self.session._cluster_manager_client.kill_all_sessions()

    def tearDown(self) -> None:
        self.session.kill()


class TestSessionCluster(TestSessionBase):
    session = None

    def setUp(self):
        pass

    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "5"}
        cls.session = get_cluster_context(options=opts)

    @staticmethod
    def store_opts(**kwargs):
        opts = {'total_partitions': 10}
        opts.update(kwargs)
        return opts

    def tearDown(self) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        cls.session.kill()


if __name__ == '__main__':
    unittest.main()
