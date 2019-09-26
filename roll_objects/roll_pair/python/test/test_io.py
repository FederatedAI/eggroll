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
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter


class TestIo(unittest.TestCase):
  _path = '/tmp/eggroll/levelDb/ns/testMapValues/0'

  def test_write_batch(self):
    input_adapter = RocksdbSortedKvAdapter(options={'path': TestIo._path})

    writebatch = input_adapter.new_batch()

    for i in range(10):
      target = str(i).encode()
      print(target)
      writebatch.put(b'k' + target, b'v' + target)

    writebatch.close()

  def test_iterate(self):
    input_adapter = RocksdbSortedKvAdapter(options={'path': TestIo._path})
    iterator = input_adapter.iteritems()

    print("path:", TestIo._path)

    for k, v in iterator:
      print(k, v)


if __name__ == '__main__':
  unittest.main()
