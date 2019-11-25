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

from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_pair.roll_pair import RollPair
import roll_paillier_tensor as rpt_engine

class RollPaillierTensorOnRollPair(object):
  def __init__(self, store_desc=None):
    self._store = RollPair(store_desc)

  def scalar_mul(self, scalar):
    def functor(ser_enc_mat, scalar):
      enc_mat_mpz = rpt_engine.load(ser_enc_mat)
      pub_key, pvt_key = rpt_engine.keygen()

      mpz_result = rpt_engine.scalar_mul(enc_mat_mpz, scalar, pub_key, pvt_key)


      decMat = rpt_engine.decrypt(mpz_result, pub_key, pvt_key)
      #resMat = rpt_engine.decode(decMat, pub_key, pvt_key)\

      # print("++++++++++")
      # rpt_engine.print(mpz_result, pub_key, pvt_key)


      return rpt_engine.dump(mpz_result)

    return self._store.map_values(lambda v: functor(v, float(scalar)))

  def add(self, other):
    def functor(left, right):
      print("[cpu_add]+++++++++++++")
      left_mpz = rpt_engine.load(left)
      right_mpz = rpt_engine.load(right)
      pub_key, pvt_key = rpt_engine.keygen()

      mpz_result = rpt_engine.add(left_mpz, right_mpz, pub_key, pvt_key)
      return rpt_engine.dump(mpz_result)

    return self._store.join(other._store, lambda left, right : functor(left, right))

  def gpu_add(self, other):
    def functor(left, right):
      pub_key, pvt_key = rpt_engine.gpu_genkey()

      enc_mat1 = rpt_engine.gpu_load(left)
      enc_mat2 = rpt_engine.gpu_load(right)
      pln_mat2 = rpt_engine.gpu_decrypt(enc_mat2, pub_key, pvt_key)

      #test [dotmul] [dotadd]
      #res = rpt_engine.gpu_dotadd(enc_mat1, enc_mat2, pub_key)
      res_mat = rpt_engine.gpu_dotmul(enc_mat1, pln_mat2, pub_key)

      res_mat_pln = rpt_engine.gpu_decrypt(res_mat, pub_key, pvt_key)

      #test [dump] [load]
      h_Byte = rpt_engine.gpu_dump(res_mat_pln)
      d_mem = rpt_engine.gpu_load(h_Byte)

      res_val = rpt_engine.gpu_decode(d_mem, pub_key)

      #mpz_result = rpt_engine.add(gpu_left_mpz, gpu_right_mpz, pub_key, pvt_key)
      #return rpt_engine.gpu_dump(mpz_result)
      return b'hello'

    return self._store.join(other._store, lambda left, right : functor(left, right))
    #return 1

  def gpu_load(self, other):
    def functor(left, right):

      #load
      gpu_left_enc = rpt_engine.gpu_load(left)
      gpu_right_enc = rpt_engine.gpu_load(right)

      #dump

      d_dump = rpt_engine.gpu_dump(gpu_left_enc)
      d_load = rpt_engine.gpu_load(d_dump)

      #mpz_result = rpt_engine.add(gpu_left_mpz, gpu_right_mpz, pub_key, pvt_key)
      #return rpt_engine.gpu_dump(mpz_result)
      return b'hello'

    return self._store.join(other._store, lambda left, right : functor(left, right))
    #return 1

  def mat_mul(self, other):
    def seq_op(left, right):
      print("cpu mat_mul here")
      pub_key, pvt_key = rpt_engine.keygen()
      enc_mat1 = rpt_engine.load(left)
      enc_mat2 = rpt_engine.load(right)
      pln_mat2 = rpt_engine.decrypt(enc_mat2, pub_key, pvt_key)

      sub_res = rpt_engine.matmul_c_eql(enc_mat1, pln_mat2, pub_key, pvt_key)
      dumping = rpt_engine.dump(sub_res)
      return dumping

    def comb_op(left, right):
      print(left)

    partition_result = self._store.join(other._store, lambda left, right : seq_op(left, right))
    # print(partition_result)
    #
    rppr = partition_result
    final_result = rppr.reduce(comb_op)
    return final_result

  # def mat_mul(self, other):
  #   def functor(left, right):
  #     print("cpu mat_mul here")
  #     pub_key, pvt_key = rpt_engine.keygen()
  #     enc_mat1 = rpt_engine.load(left)
  #     enc_mat2 = rpt_engine.load(right)
  #     pln_mat2 = rpt_engine.decrypt(enc_mat2, pub_key, pvt_key)
  #
  #     sub_res = rpt_engine.matmul_c_eql(enc_mat1, pln_mat2, pub_key, pvt_key)
  #     dumping = rpt_engine.dump(sub_res)
  #     return b'hello'
  #   return self._store.join(other._store, lambda left, right : functor(left, right))
