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
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from api import rollsite

if __name__ == '__main__':
    #ggroll.init("atest")
    rollsite.init("atest",
                  "roll_site/test/role_conf.json",
                  "roll_site/test/server_conf.json",
                  "roll_site/test/transfer_conf.json",)
    _tag = "Hello"
    #a = _tag


    #content = f.read(10000)
    #print(content)

    fp = open("testA.model", 'r')
    while True:
        print("push!!!")
        content = fp.read(35)
        if not content:
            break
        print(content)
        rollsite.push(content, "model_A", tag="{}".format(_tag))

    '''
    fp = open("testA.model", 'r')
    rollsite.push(fp, "model_ID", tag="{}".format(_tag))
    '''
    fp.close()



