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

from eggroll.api import eggroll
from eggroll.api import clustercomm

if __name__ == '__main__':
    eggroll.init("atest")
    clustercomm.init("atest", {
        "local": {
            "role": "host",
            "party_id": 10001
        },

        "role": {
            "host": [
                10001,
                10002
            ],
            "arbiter": [
                99999
            ],
            "guest": [
                10001
            ]
        }})
    for _tag in range(0, 1000, 2):
        c = eggroll.parallelize(range(_tag), partition=3, persistent=True).map(lambda k, v: (v, k+1))
        print(c)
        a = _tag
        clustercomm.remote(a, "RsaIntersectTransferVariable.rsa_pubkey", tag="{}".format(_tag))
        clustercomm.remote(c, "RsaIntersectTransferVariable.rsa_pubkey", tag="{}".format(_tag + 1))
