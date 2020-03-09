#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
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

import unittest

from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_site.test.roll_site_test_asset import get_debug_test_context, \
    get_cluster_context, get_standalone_context, default_props_file, \
    remote_parties, get_parties

props_file_host = default_props_file
#props_file_host = default_props_file + '.host'

props_file_guest = default_props_file
#props_file_guest = default_props_file + '.guest'


row_limit = 100000
obj_size = 1 << 20


def data_generator(limit):
    for i in range(limit):
        yield (f"key-{i}", f"value-{i}")


class TestRollSiteBase(unittest.TestCase):
    rs_context_host = None
    rs_context_guest = None

    _obj_rs_name = "RsaIntersectTransferVariable.rsa_pubkey"
    _obj_rs_tag = "testing_rs_obj"
    _obj = "This is the remote object in a str"

    _obj_rs_name_big = "RsaIntersectTransferVariable.rsa_pubkey.big"
    _obj_rs_tag_big = "testing_rs_obj.big"
    _obj_big = b'1' * obj_size

    _rp_rs_name = "roll_pair_name.table"
    _rp_rs_tag = "roll_pair_tag"

    _rp_rs_name_big = "roll_pair_name.table.big"
    _rp_rs_tag_big = "roll_pair_tag.big"

    _rp_rs_name_big_mp = "roll_pair_name.table.big.mp"
    _rp_rs_tag_big_mp = "roll_pair_tag.big.mp"

    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_host = get_debug_test_context(role='host',
                                                     props_file=props_file_host)
        cls.rs_context_guest = get_debug_test_context(manager_port=4671,
                                                      egg_port=20003,
                                                      transfer_port=20004,
                                                      session_id='testing_guest',
                                                      role='guest',
                                                      props_file=props_file_guest)

    def test_init(self):
        print(1)

    def test_remote(self):
        rs = self.rs_context_guest.load(name=self._obj_rs_name, tag=self._obj_rs_tag)

        futures = rs.push(self._obj, remote_parties)
        for future in futures:
            result = future.result()
            print("result:", result)

    def test_get(self):
        rs = self.rs_context_host.load(name=self._obj_rs_name, tag=self._obj_rs_tag)
        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                raise TypeError('require getting a obj but RollPair found')
            else:
                self.assertEqual(obj, self._obj, f"got wrong object. expected: {self._obj}, actual: {obj}")
                print("obj:", obj)

    def test_remote_big(self):
        rs = self.rs_context_guest.load(name=self._obj_rs_name_big, tag=self._obj_rs_tag_big)
        futures = rs.push(self._obj_big, remote_parties)
        for future in futures:
            result = future.result()
            print("result:", result)

    def test_get_big(self):
        rs = self.rs_context_host.load(name=self._obj_rs_name_big, tag=self._obj_rs_tag_big)
        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                raise TypeError('require getting a obj but RollPair found')
            else:
                self.assertEqual(obj, self._obj_big, f"got wrong object. expected len: {len(self._obj)}, actual: {len(obj)}")

    def test_remote_rollpair(self):
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")]
        rp_options = {'include_key': True}
        rp_context = self.rs_context_guest.rp_ctx
        rp = rp_context.load("namespace", "name").put_all(data, options=rp_options)

        rs = self.rs_context_guest.load(name=self._rp_rs_name, tag=self._rp_rs_tag)
        futures = rs.push(rp, remote_parties)
        for future in futures:
            result = future.result()
            print("result: ", result)

    def test_get_rollpair(self):
        rs = self.rs_context_host.load(name=self._rp_rs_name, tag=self._rp_rs_tag)
        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                key = "k1"
                value = obj.get(key)
                self.assertEqual(value, "v1", f"got wrong value. expected: 'v1', actual: {value}")
                print("obj:", obj, ", value:", value)
            else:
                raise TypeError(f'require getting a RollPair but obj found: {obj}')

    def test_remote_rollpair_big(self):
        rp_options = {'include_key': True}
        rp_context = self.rs_context_guest.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big)
        rp.put_all(data_generator(row_limit), options=rp_options)
        print(f"count: {rp.count()}")

        rs = self.rs_context_guest.load(name=self._rp_rs_name_big, tag=self._rp_rs_tag_big)
        futures = rs.push(rp, remote_parties)
        for future in futures:
            result = future.result()
            print("result: ", result)

    def test_get_rollpair_big(self):
        rs = self.rs_context_host.load(name=self._rp_rs_name_big, tag=self._rp_rs_tag_big)
        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                key = "key-1"
                value = obj.get(key)
                self.assertEqual(value, "value-1", f"got wrong value. expected: 'value-1', actual: {value}")
                print("obj:", obj, ", value:", value, ", count:", obj.count())
            else:
                raise TypeError(f'require getting a RollPair but obj found: {obj}')

    def test_remote_rollpair_big_multi_partitions(self):
        rp_options = {'include_key': True, 'total_partitions': 3}
        rp_context = self.rs_context_guest.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big_mp, options=rp_options)
        rp.put_all(data_generator(row_limit), options=rp_options)
        print(f"count: {rp.count()}")

        if rp.count() <= 100:
            print(list(rp.get_all()))

        rs = self.rs_context_guest.load(name=self._rp_rs_name_big_mp, tag=self._rp_rs_tag_big_mp)
        futures = rs.push(rp, remote_parties)
        for future in futures:
            result = future.result()
            print("result: ", result)

    def test_get_rollpair_big_multi_partitions(self):
        #rp_options = {'include_key': True, 'total_partitions': 3}
        rs = self.rs_context_host.load(name=self._rp_rs_name_big_mp, tag=self._rp_rs_tag_big_mp)

        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                key = "key-1"
                value = obj.get(key)
                #self.assertEqual(value, "value-1", f"got wrong value. expected: 'value-1', actual: {value}")
                self.assertEqual(value, "value-1", f"got wrong value. expected: 'value-1', actual: {value}")
                self.assertEqual(obj.count(), row_limit, f"got wrong count value. expected: {row_limit}, actual: {obj.count()}")
            else:
                raise TypeError(f'require getting a RollPair but obj found: {obj}')

    def test_get_table(self):
        rp_context = self.rs_context_host.rp_ctx

        rp = rp_context.load('atest', f'__federation__#atest#{self._rp_rs_name_big_mp}#{self._rp_rs_tag_big_mp}#guest#10002#host#10001')

        print(f'key-1: {rp.get("key-1")}')

        print(f'1st: {rp.take(2)}')
        print(f'count: {rp.count()}')

    def test_get_all(self):
        rp_context = self.rs_context_host.rp_ctx

        rp = rp_context.load('atest', f'__federation__#atest#{self._rp_rs_name_big_mp}#{self._rp_rs_tag_big_mp}#guest#10002#host#10001')
        print(list(rp.get_all()))

    def test_count(self):
        rp_context = self.rs_context_guest.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big_mp)

        print(rp.count())

    def test_put_all_multi_partitions(self):
        rp_options = {'include_key': True, 'total_partitions': 3}
        rp_context = self.rs_context_guest.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big_mp, options=rp_options)
        rp.put_all(data_generator(9), options=rp_options)
        print(f"count: {rp.count()}")


class TestRollSiteDebugGuest(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_guest = get_debug_test_context(manager_port=4671,
                                                      egg_port=20003,
                                                      transfer_port=20004,
                                                      session_id='testing_guest',
                                                      role='guest',
                                                      props_file=props_file_guest)

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair_big_multi_partitions()


class TestRollSiteDebugHost(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_host = get_debug_test_context(role='host',
                                                     props_file=props_file_host)

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair_big_multi_partitions()


class TestRollSiteStandalone(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_host = get_standalone_context(role='host', props_file=props_file_host)
        cls.rs_context_guest = get_standalone_context(role='guest', props_file=props_file_guest)

    def test_remote(self):
        super().test_remote()

    def test_get(self):
        super().test_get()

    def test_remote_rollpair(self):
            super().test_remote_rollpair()

    def test_get_rollpair(self):
        super().test_get_rollpair()

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair()

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair()


class TestRollSiteCluster(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "3"}
        cls.rs_context_guest = get_cluster_context(role='guest', options=opts, props_file=props_file_guest, party_id=10002)
        cls.rs_context_host = get_cluster_context(role='host', options=opts, props_file=props_file_host, party_id=10001)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rs_context_guest.rp_ctx.get_session().stop()
        cls.rs_context_host.rp_ctx.get_session().stop()

    def test_remote(self):
        super().test_remote()
    
    def test_get(self):
        super().test_get()

    def test_remote_rollpair(self):
        super().test_remote_rollpair()

    def test_get_rollpair(self):
        super().test_get_rollpair()

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair_big_multi_partitions()

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair_big_multi_partitions()


class TestRollSiteClusterGuest(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "3"}
        cls.rs_context_guest = get_cluster_context(role='guest', options=opts, props_file=props_file_guest, party_id=10002)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rs_context_guest.rp_ctx.get_session().stop()

    def test_remote(self):
        super().test_remote()

    def test_remote_rollpair(self):
        super().test_remote_rollpair()

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair_big_multi_partitions()


class TestRollSiteClusterHost(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "3"}
        cls.rs_context_host = get_cluster_context(role='host', options=opts, props_file=props_file_host, party_id=10001)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rs_context_guest.rp_ctx.get_session().stop()
        cls.rs_context_host.rp_ctx.get_session().stop()

    def test_remote(self):
        super().test_remote()

    def test_get(self):
        super().test_get()

    def test_remote_rollpair(self):
        super().test_remote_rollpair()

    def test_get_rollpair(self):
        super().test_get_rollpair()

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair_big_multi_partitions()

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair_big_multi_partitions()
