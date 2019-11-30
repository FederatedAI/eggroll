# -*- coding: utf-8 -*-
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


import roll_paillier_tensor as rpt_engine
import unittest
import pandas as pd
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter

mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat_mpi.csv").values

class TestIo(unittest.TestCase):
  _path_prefix = '/tmp/eggroll/rollpair.leveldb/namespace'
  _path = '/tmp/eggroll/levelDb/ns/testMapValues/0'

  _name_prefix = f'{_path_prefix}/name'
  _test_prefix = f'{_path_prefix}/test'

  def test_insert_matrix(self):
    #mine
    pub, priv = rpt_engine.keygen()
    zmat = rpt_engine.init(mat, pub)
    mpz_mat = rpt_engine.encrypt_and_obfuscate(zmat, pub)
    total_row = 2
    total_col = 2
    sliced = rpt_engine.slice_n_dump(mpz_mat, total_row, total_col, total_row * total_col)

    mat_path = f'{TestIo._path_prefix}/mat_a'
    for i in range(4):
      cur_path = f'{mat_path}/{i}'
      key = f'{i // total_col}_{i % total_col}'
      print(f'max: processing block {i} at path: {cur_path}. key: {key}')

      data = sliced[(i * 256 * 4) : ((i + 1) * 256 * 4)]
      input_adapter = RocksdbSortedKvAdapter(options={'path': cur_path})
      writebatch = input_adapter.new_batch()
      writebatch.put(key.encode(), data)
      writebatch.close()
      input_adapter.close()

  def test_write_batch(self):
    path_prefix = TestIo._name_prefix
    for p in range(4):
      adapter = RocksdbSortedKvAdapter(options={'path': f'{path_prefix}/{p}'})

      writebatch = adapter.new_batch()

      for i in range(10):
        target = str(i).encode()
        writebatch.put(b'k' + target, b'v' + target)

      writebatch.close()
      adapter.close()


  def test_iterate(self):
    input_adapter = RocksdbSortedKvAdapter(options={'path': TestIo._path})
    iterator = input_adapter.iteritems()

    print("path:", TestIo._path)

    for k, v in iterator:
      print(k, v)

  def test_check_result(self):
    input_adapter = RocksdbSortedKvAdapter(options={'path': TestIo.result_path})
    iterator = input_adapter.iteritems()

    print("path:", TestIo.result_path)

    for k, v in iterator:
      print(k, v)

if __name__ == '__main__':
  unittest.main()
