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
from eggroll.roll_pair.test.roll_pair_test_assets import get_debug_test_context
from eggroll.roll_site.roll_site import RollSiteContext

is_standalone = True
manager_port_guest = 4671
egg_port_guest = 20001
transfer_port_guest = 20002
manager_port_host = 4671
egg_port_host = 20001
transfer_port_host = 20002
remote_parties = [('host', '10001')]
get_parties = [('guest', '10002')]

host_partyId = 10001
host_ip = 'localhost'
host_rs_port = 9395
guest_partyId = 10002
guest_ip = 'localhost'
guest_rs_port = 9396

class TestRollSite(unittest.TestCase):
    def test_remote(self):
        rp_context = get_debug_test_context(is_standalone, manager_port_guest, egg_port_guest, transfer_port_guest, 'testing_guest')
        context = RollSiteContext("atest", self_role='guest', self_partyId=guest_partyId, rs_ip=guest_ip, rs_port=guest_rs_port, rp_ctx=rp_context)

        _tag = "Hello2"
        rs = context.load(name="RsaIntersectTransferVariable.rsa_pubkey", tag="{}".format(_tag))
        fp = open("testA.model", 'r')
        obj = fp.read(35)
        futures = rs.push(obj, remote_parties)
        fp.close()
        for future in futures:
            role, party = future.result()
            print("result:", role, party)

    def test_get(self):
        rp_context = get_debug_test_context(is_standalone, manager_port_host, egg_port_host, transfer_port_host, 'testing')
        context = RollSiteContext("atest", self_role='host', self_partyId=host_partyId, rs_ip=host_ip, rs_port=host_rs_port, rp_ctx=rp_context)
        _tag = "Hello2"
        rs = context.load(name="RsaIntersectTransferVariable.rsa_pubkey", tag="{}".format(_tag))
        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                key = 'hello'
                print("obj:", obj.get(key))
            else:
                print("obj:", obj)

    def test_remote_rollpair(self):
        rp_context = get_debug_test_context(is_standalone, manager_port_guest, egg_port_guest, transfer_port_guest, 'testing_guest')
        data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5"), ("k6", "v6")]
        context = RollSiteContext("atest2", self_role='guest', self_partyId=guest_partyId, rs_ip=guest_ip, rs_port=guest_rs_port, rp_ctx=rp_context)

        rp_options = {'include_key': True}
        rp = rp_context.load("namespace", "name").put_all(data, options=rp_options)
        _tag = "Hello"
        rs = context.load(name="roll_pair_name.table", tag="roll_pair_tag")
        futures = rs.push(rp, remote_parties)
        for future in futures:
            role, party = future.result()
            print("result:", role, party)

    def test_get_rollpair(self):
        rp_context = get_debug_test_context(is_standalone, manager_port_host, egg_port_host, transfer_port_host, 'testing')
        context = RollSiteContext("atest2", self_role='host', self_partyId=host_partyId, rs_ip=host_ip, rs_port=host_rs_port, rp_ctx=rp_context)

        _tag = "roll_pair_tag"
        rs = context.load(name="roll_pair_name.table", tag="{}".format(_tag))
        futures = rs.pull(get_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                key = "k1"
                print("obj:", obj.get(key))
            else:
                print("obj:", obj)
