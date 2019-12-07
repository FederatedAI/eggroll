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

class TestIo(unittest.TestCase):
  _path_prefix = '/tmp/eggroll/rollpair.leveldb/namespace'
  _path = '/tmp/eggroll/levelDb/ns/testMapValues/0'

  _name_prefix = f'{_path_prefix}/name'
  _test_prefix = f'{_path_prefix}/test'

  _mat_a = f'{_path_prefix}/mat_a'
  _mat_b = f'{_path_prefix}/mat_b'


  def test_insert_matrix_tmp(self):
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

  def test_insert_matrix(self):
    # run in roll
    # reads csv and splits it
    blockDimx = 2
    blockDimy = 2

    b_mat1, tag1, row1, col1 = rpt_engine.slice_csv(mat, blockDimx, blockDimy)
    b_mat2, tag2, row2, col2 = rpt_engine.slice_csv(mat, blockDimx, blockDimy)

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
    pub, priv = rpt_engine.keygen()

    mat_a_path = TestIo._mat_a
    mat_b_path = TestIo._mat_b

    def do_insert(sendMat, mat_path):
      i = 0
      data_list = []
      for plain_data in sendMat:
        # plain received => plain mpz
        zmat = rpt_engine.init_byte_csv(plain_data, pub)
        print(plain_data)

        # plain mpz => cipher mpz => dump
        cipher = rpt_engine.encrypt_and_obfuscate(zmat, pub)
        dump_cipher = rpt_engine.dump(cipher)

        cur_path = f'{mat_path}/{i}'
        print((i // blockNumx), (i % blockNumy))
        key = f'{i // blockNumx}_{i % blockNumy}'
        print(f'max: processing block {i} at path: {cur_path}. key: {key}')

        print(f'max: processing block {i} at path: {cur_path}. key: {key}')
        data_list.append((key, dump_cipher))
        i += 1

      rptc.load("ns", "n").put_all(data_list)

    do_insert(sendMat1, TestIo._mat_a)
    do_insert(sendMat2, TestIo._mat_b)

  def test_insert_matrix2(self):
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
    sendMat = []
    sendVec = []

    headSize = 4 * 8
    subMatsize = 2 * 2 * 8;
    subVecsize = 2 * 1 * 8;

    blockMatNumx = col1 // blockMatDimx
    blockMatNumy = row1 // blockMatDimy

    blockVecNumx = col2 // blockVecDimx
    blockVecNumy = row2 // blockVecDimy


    for i in range(4):
      sendMat.append(mat_head + b_mat[(headSize + i * subMatsize)
                                      : ((i + 1) * subMatsize + headSize)])
      sendVec.append(vec_head + b_vec[(headSize + int(i % 2) * subVecsize)
                                      : ((int(i % 2) + 1) * subVecsize + headSize)])

    # todo: add send logic here
    # run in egg
    pub, priv = rpt_engine.keygen()

    mat_a_path = TestIo._mat_a
    mat_b_path = TestIo._mat_b

    def do_insert(sendMat, mat_path, blockNumx, blockNumy):
      i = 0
      for plain_data in sendMat:
        # plain received => plain mpz
        zmat = rpt_engine.init_byte_csv(plain_data, pub)
        print(plain_data)

        # plain mpz => cipher mpz => dump
        cipher = rpt_engine.encrypt_and_obfuscate(zmat, pub)
        dump_cipher = rpt_engine.dump(cipher)

        cur_path = f'{mat_path}/{i}'
        print("************",(i // blockNumx), (i % blockNumy))
        key = f'{i // blockNumx}_{i % blockNumy}'
        print(f'max: processing block {i} at path: {cur_path}. key: {key}')

        input_adapter = RocksdbSortedKvAdapter(options={'path': cur_path})
        writebatch = input_adapter.new_batch()

        #print(dump_cipher)

        # dumps
        writebatch.put(key.encode(), dump_cipher)
        writebatch.close()
        input_adapter.close()
        i += 1

    do_insert(sendMat, TestIo._mat_a, blockMatNumx, blockMatNumy)
    do_insert(sendVec, TestIo._mat_b, blockMatNumx, blockMatNumy)

  def test_insert_matrix3(self):
    # run in roll
    # reads csv and splits it

    blockMatDimx = 20
    blockMatDimy = 25

    blockVecDimx = 12
    blockVecDimy = 25

    b_mat, tag1, row1, col1 = rpt_engine.slice_csv(brest_G, blockMatDimx, blockMatDimy)
    b_vec, tag2, row2, col2 = rpt_engine.slice_csv(brest_H, blockVecDimx, blockVecDimy)

    mat_head = rpt_engine.make_header(blockMatDimx, blockMatDimy, 3072, tag1)
    vec_head = rpt_engine.make_header(blockVecDimx, blockVecDimy, 3072, tag2)

    # sendMat1 contains sliced plain data
    sendMat = []
    sendVec = []

    blockMatNumx = (col1 + blockMatDimx - 1) // blockMatDimx
    blockMatNumy = (row1 + blockMatDimy - 1) // blockMatDimy

    blockVecNumx = (col2 + blockVecDimx - 1) // blockVecDimx
    blockVecNumy = (row2 + blockVecDimy - 1) // blockVecDimy

    print("++++++++++++",blockMatNumx)
    print("++++++++++++",blockMatNumy)
    print("++++++++++++",blockVecNumx)
    print("++++++++++++",blockVecNumy)

    headSize = 4 * 8
    subMatsize = blockMatDimx * blockMatDimy * 8;
    subVecsize = blockVecDimx * blockVecDimy * 8;

    print("subMatsize = ",subMatsize)
    print("subVecsize = ",subVecsize)


    for i in range(4):
      sendMat.append(mat_head + b_mat[(headSize + i * subMatsize)
                                      : ((i + 1) * subMatsize + headSize)])
      sendVec.append(vec_head + b_vec[(headSize + int(i % 2) * subVecsize)
                                      : ((int(i % 2) + 1) * subVecsize + headSize)])

    # todo: add send logic here
    # run in egg
    pub, priv = rpt_engine.keygen()

    mat_a_path = TestIo._mat_a
    mat_b_path = TestIo._mat_b

    def do_insert(sendMat, mat_path, blockNumx, blockNumy):
      i = 0
      data_list = []
      for plain_data in sendMat:
        # plain received => plain mpz
        zmat = rpt_engine.init_byte_csv(plain_data, pub)

        # plain mpz => cipher mpz => dump
        cipher = rpt_engine.encrypt_and_obfuscate(zmat, pub)
        dump_cipher = rpt_engine.dump(cipher)

        cur_path = f'{mat_path}/{i}'
        print("************",(i // blockNumx), (i % blockNumy))
        key = f'{i // blockNumx}_{i % blockNumy}'
        print(f'max: processing block {i} at path: {cur_path}. key: {key}')
        data_list.append((key, dump_cipher))
        i += 1

      rptc.load("a1", "b").put_all(data_list)

    do_insert(sendMat, TestIo._mat_a, blockMatNumx, blockMatNumy)
    do_insert(sendVec, TestIo._mat_b, blockMatNumx, blockMatNumy)


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
