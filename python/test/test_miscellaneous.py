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

import unittest
from time import monotonic

from eggroll.core.utils import hash_code


class TestMiscellaneous(unittest.TestCase):

    def test_hash_code(self):
        start = monotonic()
        data = ('1' * 1000).encode()
        for i in range(100000):
            if i % 1000 == 0:
                print(i)
            hash_code(data)

        end = monotonic()

        print(end - start)
