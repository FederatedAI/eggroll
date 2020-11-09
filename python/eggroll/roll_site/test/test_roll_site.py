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
import argparse
import sys
import unittest

import grpc

from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_site.test.roll_site_test_asset import get_debug_test_context, \
    get_cluster_context, get_standalone_context, default_props_file
from eggroll.utils.log_utils import get_logger

L = get_logger()

props_file_get = default_props_file
#props_file_get = default_props_file + '.host'

props_file_remote = default_props_file
#props_file_remote = default_props_file + '.guest'


row_limit = 100000
obj_size = 1 << 20


def data_generator(limit):
    for i in range(limit):
        yield (f"key-{i}", f"value-{i}")


def args_to_testcase(testcase_class, func_name=None, src_party_id=None, dst_party_id=None, job_id=None):
    testloader = unittest.TestLoader()
    testnames = testloader.getTestCaseNames(testcase_class)
    suite = unittest.TestSuite()
    if func_name is None:
        for name in testnames:
            suite.addTest(testcase_class(name, src_party_id=src_party_id, dst_party_id=dst_party_id, job_id=job_id))
    else:
        for name in testnames:
            if func_name == name:
                suite.addTest(testcase_class(name, src_party_id=src_party_id, dst_party_id=dst_party_id, job_id=job_id))
    return suite


class TestRollSiteBase(unittest.TestCase):
    rs_context_get = None
    rs_context_remote = None

    self_party_id = None

    _obj_rs_name = "RsaIntersectTransferVariable.rsa_pubkey"
    _obj_rs_tag = "testing_rs_obj"
    _obj = "This is the remote object in a str"

    _obj_rs_name_big = "RsaIntersectTransferVariable.rsa_pubkey.big"
    _obj_rs_tag_big = "testing_rs_obj.big"
    _obj_big = '1234567890' * (obj_size // 10)

    _rp_rs_name = "roll_pair_name.table"
    _rp_rs_tag = "roll_pair_tag"

    _rp_rs_name_big = "roll_pair_name.table.big"
    _rp_rs_tag_big = "roll_pair_tag.big"

    _rp_rs_name_big_mp = "roll_pair_name.table.big.mp"
    _rp_rs_tag_big_mp = "roll_pair_tag.big.mp"

    src_party_id = None
    dst_party_id = None
    job_id = None

    def __init__(self, methodName='runTest', src_party_id=10002, dst_party_id=10001, job_id=None):
        super(TestRollSiteBase, self).__init__(methodName)
        TestRollSiteBase.src_party_id = src_party_id
        TestRollSiteBase.dst_party_id = dst_party_id
        TestRollSiteBase.job_id = job_id
        self.get_parties = [("src", src_party_id)]
        self.remote_parties = [("dst", dst_party_id)]

    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_get = get_debug_test_context(manager_port=4670,
                                                    command_port=20001,
                                                    transfer_port=20002,
                                                    role='dst',
                                                    self_party_id=10001,
                                                    props_file=props_file_get,
                                                    roll_site_session_id="atest")

        cls.rs_context_remote = get_debug_test_context(manager_port=4671,
                                                       command_port=20003,
                                                       transfer_port=20004,
                                                       role='src',
                                                       self_party_id=10002,
                                                       props_file=props_file_remote,
                                                       roll_site_session_id="atest")

    def test_init(self):
        print(1)

    def test_remote(self):
        rs = self.rs_context_remote.load(name=self._obj_rs_name, tag=self._obj_rs_tag)

        futures = rs.push(self._obj, self.remote_parties)
        for future in futures:
            result = future.result()
            print("result:", result)

    def test_get(self):
        rs = self.rs_context_get.load(name=self._obj_rs_name, tag=self._obj_rs_tag)

        futures = rs.pull(self.get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                raise TypeError('require getting a obj but RollPair found')
            else:
                self.assertEqual(obj, self._obj, f"got wrong object. expected: {self._obj}, actual: {obj}")
                print("obj:", obj)

    def test_remote_big(self):
        rs = self.rs_context_remote.load(name=self._obj_rs_name_big, tag=self._obj_rs_tag_big)
        futures = rs.push(self._obj_big, self.remote_parties)
        for future in futures:
            result = future.result()
            print("result:", result)

    def test_get_big(self):
        rs = self.rs_context_get.load(name=self._obj_rs_name_big, tag=self._obj_rs_tag_big)
        futures = rs.pull(self.get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                raise TypeError('require getting a obj but RollPair found')
            else:
                self.assertEqual(obj, self._obj_big, f"got wrong object. expected len: {len(self._obj)}, actual: {len(obj)}")

    def test_remote_rollpair(self):
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")]
        rp_options = {'include_key': True}
        rp_context = self.rs_context_remote.rp_ctx
        options = {'create_if_missing': True}
        rp = rp_context.load("namespace", "name", options={"create_if_missing": True}).put_all(data, options=rp_options)

        rs = self.rs_context_remote.load(name=self._rp_rs_name, tag=self._rp_rs_tag)
        futures = rs.push(rp, self.remote_parties)
        for future in futures:
            result = future.result()
            print("result: ", result)

    def test_get_rollpair(self):
        rs = self.rs_context_get.load(name=self._rp_rs_name, tag=self._rp_rs_tag)
        futures = rs.pull(self.get_parties)
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
        rp_options.update(create_if_missing=True)
        rp_context = self.rs_context_remote.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big, options=rp_options)
        rp.put_all(data_generator(row_limit), options=rp_options)
        print(f"count: {rp.count()}")

        rs = self.rs_context_remote.load(name=self._rp_rs_name_big, tag=self._rp_rs_tag_big)
        futures = rs.push(rp, self.remote_parties)
        for future in futures:
            result = future.result()
            print("result: ", result)

    def test_get_rollpair_big(self):
        rs = self.rs_context_get.load(name=self._rp_rs_name_big, tag=self._rp_rs_tag_big)
        futures = rs.pull(self.get_parties)
        for future in futures:
            obj = future.result()
            last_key = row_limit - 1
            if isinstance(obj, RollPair):
                key = f"key-{last_key}"
                value = obj.get(key)
                self.assertEqual(row_limit, obj.count(), f'got wrong count. expect: {row_limit}, actual: {obj.count()}')
                if row_limit > 0:
                    self.assertEqual(value, f"value-{last_key}", f"got wrong value. expected: 'value-{last_key}', actual: {value}")
                else:
                    self.assertEqual(value, None, f"got wrong value. expected: None, actual: {value}")
                print("obj:", obj, ", value:", value, ", count:", obj.count())
            else:
                raise TypeError(f'require getting a RollPair but obj found: {obj}')

    def test_remote_rollpair_big_multi_partitions(self):
        rp_options = {'include_key': True, 'total_partitions': 3, 'create_if_missing': True}
        rp_options.update(create_if_missing=True)
        rp_context = self.rs_context_remote.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big_mp, options=rp_options)
        rp.put_all(data_generator(row_limit), options=rp_options)
        print(f"count: {rp.count()}")

        if rp.count() <= 100:
            print(list(rp.get_all()))

        rs = self.rs_context_remote.load(name=self._rp_rs_name_big_mp, tag=self._rp_rs_tag_big_mp)
        futures = rs.push(rp, self.remote_parties)
        for future in futures:
            result = future.result()
            print("result: ", result)

    def test_get_rollpair_big_multi_partitions(self):
        #rp_options = {'include_key': True, 'total_partitions': 3}
        rs = self.rs_context_get.load(name=self._rp_rs_name_big_mp, tag=self._rp_rs_tag_big_mp)

        futures = rs.pull(self.get_parties)
        for future in futures:
            obj = future.result()
            last_key = row_limit - 1
            if isinstance(obj, RollPair):
                key = f"key-{last_key}"
                value = obj.get(key)
                #self.assertEqual(value, "value-1", f"got wrong value. expected: 'value-1', actual: {value}")
                self.assertEqual(obj.count(), row_limit, f"got wrong count value. expected: {row_limit}, actual: {obj.count()}")
                self.assertEqual(value, f"value-{last_key}", f"got wrong value. expected: 'value-{last_key}', actual: {value}")

            else:
                raise TypeError(f'require getting a RollPair but obj found: {obj}')

    def test_get_table(self):
        rp_context = self.rs_context_get.rp_ctx

        rp = rp_context.load('atest', f'__rskey#testing#{self._rp_rs_name_big_mp}#{self._rp_rs_tag_big_mp}#guest#10002#host#10001')

        print(f'key-1: {rp.get("key-1")}')

        print(f'1st: {rp.take(2)}')
        print(f'count: {rp.count()}')

    def test_get_all(self):
        rp_context = self.rs_context_get.rp_ctx

        rp = rp_context.load('atest', f'__federation__#atest#{self._rp_rs_name_big_mp}#{self._rp_rs_tag_big_mp}#guest#10002#host#10001')
        print(list(rp.get_all()))

    def test_count(self):
        rp_context = self.rs_context_remote.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big_mp)

        print(rp.count())

    def test_put_all_multi_partitions(self):
        rp_options = {'include_key': True, 'total_partitions': 3}
        rp_options.update(create_if_missing=True)
        rp_context = self.rs_context_remote.rp_ctx
        rp = rp_context.load("namespace", self._rp_rs_name_big_mp, options=rp_options)
        rp.put_all(data_generator(9), options=rp_options)
        print(f"count: {rp.count()}")

    def test_ping(self):
        remote_stub = self.rs_context_remote.stub
        get_stub = self.rs_context_get.stub
        req = proxy_pb2.Packet(header=proxy_pb2.Metadata(operator='ping', dst=proxy_pb2.Topic(partyId=self.rs_context_get.party_id)))

        L.info("starting ping")
        resp = remote_stub.unaryCall(req)
        L.info(f"ping finished {resp.header.operator}")
        self.assertEqual('pong', resp.header.operator)

        L.info("starting ping reverse")
        req = proxy_pb2.Packet(header=proxy_pb2.Metadata(operator='ping', dst=proxy_pb2.Topic(partyId=self.rs_context_remote.party_id)))
        resp = get_stub.unaryCall(req)
        L.info(f"ping finished {resp.header.operator}")
        self.assertEqual('pong', resp.header.operator)


class TestRollSiteDebugRemote(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_remote = get_debug_test_context(manager_port=4671,
                                                       command_port=20003,
                                                       transfer_port=20004,
                                                       role='src',
                                                       props_file=props_file_remote,
                                                       roll_site_session_id=cls.job_id)

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair_big_multi_partitions()


class TestRollSiteDebugGet(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_get = get_debug_test_context(manager_port=4670,
                                                    command_port=20001,
                                                    transfer_port=20002,
                                                    role='dst',
                                                    props_file=props_file_get,
                                                    roll_site_session_id=cls.job_id)

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair_big_multi_partitions()


class TestRollSiteStandaloneRemote(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_remote = get_standalone_context(role='src', self_party_id=cls.src_party_id, props_file=props_file_remote)

    def test_remote(self):
        super().test_remote()

    def test_remote_rollpair(self):
        super().test_remote_rollpair()

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair()

    def test_get(self):
        pass

    def test_get_all(self):
        pass

    def test_get_big(self):
        pass

    def test_get_rollpair(self):
        pass

    def test_get_rollpair_big(self):
        pass

    def test_get_rollpair_big_multi_partitions(self):
        pass

    def test_get_table(self):
        pass

    def test_get_tables(self):
        pass


class TestRollSiteStandaloneGet(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rs_context_get = get_standalone_context(role='dst', self_party_id=cls.dst_party_id, props_file=props_file_get)

    def test_get(self):
        super().test_get()

    def test_get_rollpair(self):
        super().test_get_rollpair()

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair()

    def test_remote(self):
        pass

    def test_remote_big(self):
        pass

    def test_remote_rollpair(self):
        pass

    def test_remote_rollpair_big(self):
        pass

    def test_remote_rollpair_big_multi_partitions(self):
        pass

    def test_count(self):
        pass

    def test_put_all_multi_partitions(self):
        pass


class TestRollSiteCluster(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "3"}
        cls.rs_context_remote = get_cluster_context(role='guest', options=opts, props_file=props_file_remote, party_id=10002)
        cls.rs_context_get = get_cluster_context(role='host', options=opts, props_file=props_file_get, party_id=10001)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rs_context_remote.rp_ctx.get_session().stop()
        cls.rs_context_get.rp_ctx.get_session().stop()

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


class TestRollSiteClusterRemote(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "16"}
        cls.rs_context_remote = get_cluster_context(role='src', options=opts, props_file=props_file_remote, party_id=cls.src_party_id, roll_site_session_id=cls.job_id)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rs_context_remote.rp_ctx.get_session().stop()

    def test_remote(self):
        super().test_remote()

    def test_remote_rollpair(self):
        super().test_remote_rollpair()

    def test_remote_rollpair_big_multi_partitions(self):
        super().test_remote_rollpair_big_multi_partitions()

    def test_get(self):
        pass

    def test_get_all(self):
        pass

    def test_get_big(self):
        pass

    def test_get_rollpair(self):
        pass

    def test_get_rollpair_big(self):
        pass

    def test_get_rollpair_big_multi_partitions(self):
        pass

    def test_get_table(self):
        pass

    def test_get_tables(self):
        pass

class TestRollSiteClusterGet(TestRollSiteBase):
    @classmethod
    def setUpClass(cls) -> None:
        opts = {"eggroll.session.processors.per.node": "16"}
        cls.rs_context_get = get_cluster_context(role='dst',
                                                 options=opts,
                                                 props_file=props_file_get,
                                                 party_id=cls.dst_party_id,
                                                 roll_site_session_id=cls.job_id)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rs_context_get.rp_ctx.get_session().stop()

    def test_get(self):
        super().test_get()

    def test_get_rollpair(self):
        super().test_get_rollpair()

    def test_get_rollpair_big_multi_partitions(self):
        super().test_get_rollpair_big_multi_partitions()

    def test_remote(self):
        pass

    def test_remote_big(self):
        pass

    def test_remote_rollpair(self):
        pass

    def test_remote_rollpair_big(self):
        pass

    def test_remote_rollpair_big_multi_partitions(self):
        pass

    def test_count(self):
        pass

    def test_put_all_multi_partitions(self):
        pass


import hashlib
class TestRollSiteRouteTable(unittest.TestCase):
    def _get_salt(self):
        import time
        return str(int(time.time() * 1000))

    def test_get_route_table(self, point='localhost:9470', party='10001', key="shiqili"):
        channel = grpc.insecure_channel(point)
        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
        salt = self._get_salt()
        md5hash = hashlib.md5((salt + key).encode("utf-8")).hexdigest()
        topic_dst = proxy_pb2.Topic(partyId=party)
        metadata = proxy_pb2.Metadata(dst=topic_dst, operator="get_route_table")
        data = proxy_pb2.Data(key=md5hash, value=salt.encode('utf-8'))
        packet = proxy_pb2.Packet(header=metadata, body=data)
        ret_packet = stub.unaryCall(packet)
        print(ret_packet.body.value.decode('utf8'))
        return ret_packet.body.value.decode('utf8')


    def test_set_route_table(self, point='localhost:9470', party='10001', key="shiqili"):
        route_table_path = 'route_table_test.json'
        with open(route_table_path, 'r') as fp:
            route_table_content = fp.read()

        salt = self._get_salt()
        route_table_content = salt + route_table_content

        md5hash = hashlib.md5((route_table_content + key).encode("utf-8")).hexdigest()

        channel = grpc.insecure_channel(point)
        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

        topic_dst = proxy_pb2.Topic(partyId=party)
        metadata = proxy_pb2.Metadata(dst=topic_dst, operator="set_route_table")

        data = proxy_pb2.Data(key=md5hash, value=route_table_content.encode('utf-8'))
        packet = proxy_pb2.Packet(header=metadata, body=data)

        ret_packet = stub.unaryCall(packet)

        print(ret_packet.body.value.decode('utf8'))
        return ret_packet.body.value.decode('utf8')


def option():
    """examples:\n\tremote obj from 10002 to 10001:\n\t\tpython test_roll_site.py -c TestRollSiteClusterRemote -f test_remote -s 10002 -d 10001 -j test01
    get obj from 10001 at 10002:\n\t\tpython test_roll_site.py -c TestRollSiteClusterGet -f test_get -s 10002 -d 10001 -j test01
    -j should use the same value in get / remote site, and always use a new -j value in every test.
    """
    print(option.__doc__)


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


if __name__ == '__main__':
    option()
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--class_name", required=True, type=str, help="the class to test")
    parser.add_argument("-f", "--function", required=False, type=str, help="the function to test")
    parser.add_argument("-s", "--src_party_id", required=False, type=str, help="source party_id, get from eggroll.properties if it is None")
    parser.add_argument("-d", "--dst_party_id", required=True, type=str, help="destination party_id")
    parser.add_argument("-j", "--job_id", required=True, type=str, help="job_id/roll_site_header_id")

    args = parser.parse_args()
    class_name_str = args.class_name
    func_name = args.function
    src_party_id = args.src_party_id
    dst_party_id = args.dst_party_id
    job_id = args.job_id

    suite = unittest.TestSuite()
    suite.addTest(args_to_testcase(str_to_class(class_name_str), func_name=func_name, src_party_id=src_party_id, dst_party_id=dst_party_id, job_id=args.job_id))

    unittest.TextTestRunner(verbosity=2).run(suite)
