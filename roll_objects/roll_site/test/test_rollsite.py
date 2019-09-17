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
    rollsite.init("atest", "role_conf", "eggroll/conf/server_conf.json")
    _tag = "Hello"
    a = _tag
    rollsite.push_sync(a, "test_push_name", tag="{}".format(_tag))
    rollsite.pull_sync(a, "test_pull_name", tag="{}".format(_tag))


