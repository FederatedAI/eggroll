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

from api import rollsite

if __name__ == '__main__':
    #ggroll.init("atest")
    rollsite.init("atest", "roll_site/test/role_conf.json", "roll_site/test/server_conf.json")
    _tag = "Hello"
    a = _tag

    f = open('write_demo.modle', 'w')
    #每次读len长度的内容，内部可能读多个packet
    while True:
        content = rollsite.pull("model_A", tag="{}".format(_tag))
        if not content:
            break
        print(content)
        f.write(content.decode())

