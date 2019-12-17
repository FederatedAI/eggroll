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

from eggroll.core.session import ErSession
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptContext
from eggroll.roll_pair.roll_pair import RollPairContext

import roll_paillier_tensor as rpt_engine
import unittest
import pandas as pd
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter

#mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat_mpi.csv").values


session = ErSession(options={"eggroll.deploy.mode": "standalone"})
rptc = RptContext(RollPairContext(session))


mat = pd.read_csv("/data/czn/data/testGemmMat.csv").values
vec = pd.read_csv("/data/czn/data/testGemmVec.csv").values
#test lr
brest_G = pd.read_csv("/data/czn/data/breast_a_egr.csv").values
brest_H = pd.read_csv("/data/czn/data/breast_b_egr.csv").values
brest_Y = pd.read_csv("/data/czn/data/breast_b_y_egr.csv").values

#mini
brest_G_mini = pd.read_csv("/data/czn/data/breast_a_egr_mini.csv").values
brest_H_mini = pd.read_csv("/data/czn/data/breast_b_egr_mini.csv").values
brest_Y_mini = pd.read_csv("/data/czn/data/breast_b_y_egr_mini.csv").values
brest_G_py = pd.read_csv("/data/czn/data/breast_a_egr_py.csv").values
brest_H_py = pd.read_csv("/data/czn/data/breast_b_egr_py.csv").values

def do_insert(sendMat, dir):
  i = 0
  data_list = []
  pub, priv = rpt_engine.keygen()
  for plain_data in sendMat:
    # plain received => plain mpz
    zmat = rpt_engine.init_byte_csv(plain_data, pub)

    # plain mpz => cipher mpz => dump
    cipher = rpt_engine.encrypt_and_obfuscate(zmat, pub)
    dump_cipher = rpt_engine.dump(cipher)

    key = f'{i // 2}_{i % 2}'
    data_list.append((key, dump_cipher))
    i += 1

  rptc.load("ns", dir.split('/')[-1]).put_all(data_list, options={"include_key": True})


class TestIo(unittest.TestCase):
  _path_prefix = '/tmp/eggroll/rollpair.lmdb/ns'
  _path = '/tmp/eggroll/levelDb/ns/testMapValues/0'

  _name_prefix = f'{_path_prefix}/name'
  _test_prefix = f'{_path_prefix}/test'

  _mat_a = f'{_path_prefix}/mat_a'
  _mat_b = f'{_path_prefix}/mat_b'
  _mat_y = f'{_path_prefix}/mat_y'
  _mat_a_py = f'{_path_prefix}/mat_a_py'
  _mat_b_py = f'{_path_prefix}/mat_b_py'


  def test_insert_matrix1(self):
    # run in roll
    # reads csv and splits it
    blockDimx = 2
    blockDimy = 2

    b_mat1, tag1, row1, col1 = rpt_engine.slice_csv(mat, blockDimx, blockDimy)
    b_mat2, tag2, row2, col2 = rpt_engine.slice_csv(vec, blockDimx, blockDimy)

    sub_head = rpt_engine.make_header(blockDimx, blockDimy, 3072, tag1)

    # sendMat1 contains sliced plain data
    sendMat1 = []
    sendMat2 = []

    subsize = 2 * 2 * 8;
    hsize = 4 * 8

    blockNumx = col1 // blockDimx
    blockNumy = row1 // blockDimy

    for i in range(4):
      sendMat1.append(sub_head + b_mat1[(hsize + i * subsize) : ((i + 1) * subsize + hsize)])
      sendMat2.append(sub_head + b_mat2[(hsize + i * subsize) : ((i + 1) * subsize + hsize)])

    # todo: add send logic here

    # run in egg
    do_insert(sendMat1, TestIo._mat_a)
    do_insert(sendMat2, TestIo._mat_b)

  def test_insert_matrix3(self):
    # run in roll
    # reads csv and splits it

    blockMatDimx = 2
    blockMatDimy = 2

    blockVecDimx = 2
    blockVecDimy = 1

    b_mat, tag1, row1, col1 = rpt_engine.slice_csv(mat, blockMatDimx, blockMatDimy)
    b_vec, tag2, row2, col2 = rpt_engine.slice_csv(vec, blockVecDimx, blockVecDimy)

    mat_head = rpt_engine.make_header(blockMatDimx, blockMatDimy, 3072, tag1)
    vec_head = rpt_engine.make_header(blockVecDimx, blockVecDimy, 3072, tag2)

    # sendMat1 contains sliced plain data
    print(vec_head)
    print("mat_head", blockMatDimx, blockMatDimy)
    print("vec_head", blockVecDimx, blockVecDimy)

    sendMat = []
    sendVec = []

    headSize = 4 * 8
    subMatsize = 2 * 2 * 8;
    subVecsize = 2 * 1 * 8;

    for i in range(4):
      sendMat.append(mat_head + b_mat[(headSize + i * subMatsize)
                                      : ((i + 1) * subMatsize + headSize)])
      sendVec.append(vec_head + b_vec[(headSize + int(i % 2) * subVecsize)
                                      : ((int(i % 2) + 1) * subVecsize + headSize)])

    # todo: add send logic here
    # run in egg
    do_insert(sendMat, TestIo._mat_a)
    do_insert(sendVec, TestIo._mat_b)

  def test_insert_matrix2(self):

    blockMatDimx = 20
    blockMatDimy = 25

    blockVecDimx = 10
    blockVecDimy = 25

    blockYDimx = 1
    blockYDimy = 25

    b_mat, tag1, row1, col1 = rpt_engine.slice_csv(brest_G, blockMatDimx, blockMatDimy)
    b_vec, tag2, row2, col2 = rpt_engine.slice_csv(brest_H, blockVecDimx, blockVecDimy)
    b_y, tag3, row3, col3 = rpt_engine.slice_csv(brest_Y, blockYDimx, blockYDimy)

    mat_head = rpt_engine.make_header(blockMatDimx, blockMatDimy, 3072, tag1)
    vec_head = rpt_engine.make_header(blockVecDimx, blockVecDimy, 3072, tag2)
    y_head = rpt_engine.make_header(blockYDimx, blockYDimy, 3072, tag3)

    # sendMat1 contains sliced plain data
    sendMat = []
    sendVec = []
    sendY = []

    headSize = 4 * 8
    subMatsize = blockMatDimx * blockMatDimy * 8;
    subVecsize = blockVecDimx * blockVecDimy * 8;
    subYsize = blockYDimx * blockYDimy * 8;

    print("subMatsize = ",subMatsize)
    print("subVecsize = ",subVecsize)
    print("subVecsize = ",subYsize)


    for i in range(4):
      sendMat.append(mat_head + b_mat[(headSize + i * subMatsize)
                                      : ((i + 1) * subMatsize + headSize)])
      sendVec.append(vec_head + b_vec[(headSize + int(i % 2) * subVecsize)
                                      : ((int(i % 2) + 1) * subVecsize + headSize)])
      sendY.append(y_head + b_y[(headSize + i * subYsize)
                                : ((i + 1) * subYsize + headSize)])

    pub, priv = rpt_engine.keygen()

    do_insert(sendMat, TestIo._mat_a)
    do_insert(sendVec, TestIo._mat_b)
    do_insert(sendY, TestIo._mat_y)

  def test_insert_matrix(self):

    blockMatDimx = 20
    blockMatDimy = 1

    blockVecDimx = 10
    blockVecDimy = 1

    blockYDimx = 1
    blockYDimy = 1

    b_mat, tag1, row1, col1 = rpt_engine.slice_csv(brest_G_mini, blockMatDimx, blockMatDimy)
    b_vec, tag2, row2, col2 = rpt_engine.slice_csv(brest_H_mini, blockVecDimx, blockVecDimy)
    b_y, tag3, row3, col3 = rpt_engine.slice_csv(brest_Y_mini, blockYDimx, blockYDimy)

    mat_head = rpt_engine.make_header(blockMatDimx, blockMatDimy, 3072, tag1)
    vec_head = rpt_engine.make_header(blockVecDimx, blockVecDimy, 3072, tag2)
    y_head = rpt_engine.make_header(blockYDimx, blockYDimy, 3072, tag3)

    # sendMat1 contains sliced plain data
    sendMat = []
    sendVec = []
    sendY = []

    headSize = 4 * 8
    subMatsize = blockMatDimx * blockMatDimy * 8;
    subVecsize = blockVecDimx * blockVecDimy * 8;
    subYsize = blockYDimx * blockYDimy * 8;

    print("subMatsize = ",subMatsize)
    print("subVecsize = ",subVecsize)
    print("subVecsize = ",subYsize)


    for i in range(4):
      sendMat.append(mat_head + b_mat[(headSize + i * subMatsize)
                                      : ((i + 1) * subMatsize + headSize)])
      sendVec.append(vec_head + b_vec[(headSize + int(i % 2) * subVecsize)
                                      : ((int(i % 2) + 1) * subVecsize + headSize)])
      sendY.append(y_head + b_y[(headSize + i * subYsize)
                                : ((i + 1) * subYsize + headSize)])

    do_insert(sendMat, TestIo._mat_a)
    do_insert(sendVec, TestIo._mat_b)
    do_insert(sendY, TestIo._mat_y)

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
