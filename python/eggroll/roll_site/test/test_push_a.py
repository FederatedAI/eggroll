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
from eggroll.roll_site.roll_site import RollSite


class TestRemote(unittest.TestCase):
    def test_remote(self):
        options = {'runtime_conf_path': 'python/eggroll/roll_site/conf/role_conf.json',
                   'server_conf_path': 'python/eggroll/roll_site/conf/server_conf.json',
                   'transfer_conf_path': 'python/eggroll/roll_site/conf/transfer_conf.json'}
        rt = RollSite("atest", options=options)
        _tag = "Hello"
        fp = open("testA.model", 'r')
        while True:
            content = fp.read(35)
            if not content:
                break
            print(content)
            rt.push(content, "model_A", tag="{}".format(_tag))

        '''
        fp = open("testA.model", 'r')
        rollsite.push(fp, "model_ID", tag="{}".format(_tag))
        '''
        fp.close()



