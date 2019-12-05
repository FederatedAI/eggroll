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

from eggroll.roll_pair.roll_pair import RollPairContext

class RptBaseEngine:
    def load(self, x):
        raise NotImplementedError("todo")
    def add(self, x, y):
        raise NotImplementedError("todo")

class RptGpuEngine(RptBaseEngine):
    def __init__(self):
        self._under = rpt_engine
    def add(self, o):
        self._under.gpu_add(o)

class RptCpuEngine(RptBaseEngine):
    def __init__(self, pub_key=None, priv_key=None):
        print("+++++++++++++++++++++3")
        self.pub_key = rpt_engine.load_pub_key(pub_key)
        self.priv_key = rpt_engine.load_prv_key(priv_key)

    # def __setstate__(self, state):
    #     bin_pub,bin_prv = state
    #     self.priv_key=rpt_engine.load_prv_key(bin_pub)
    #     self.pub_key=rpt_engine.load_pub_key(bin_prv)
    #
    # def __getstate__(self):
    #     rpt_engine.dump_pub_key(self.pub_key)
    #     rpt_engine.dump_prv_key(self.priv_key)

    def __transient__(self):
        return []

    def load(self,x):
        return rpt_engine.load(x)

    def dump(self, x):
        return rpt_engine.dump(x)

    def scalar_mul(self, x, scale):
        return rpt_engine.add(x, scale, self.pub_key, self.priv_key)

    def obf(self, x):
        return rpt_engine.obf(x, self.pub_key, self.priv_key)

    def add(self, x, y):
        return rpt_engine.add(x, y, self.pub_key, self.priv_key)

    def vdot(self, x, y):
        return rpt_engine.vdot(x, y, self.pub_key, self.priv_key)

    def encrypt(self, x):
        return rpt_engine.encrypt(x, self.pub_key, self.priv_key)

    def decrypt(self, x):
        return rpt_engine.decrypt(x, self.pub_key, self.priv_key)

class RptMixedEngine(RptBaseEngine):
    pass

class RptContext:
  def __init__(self, rp_ctx:RollPairContext):
    self.rp_ctx = rp_ctx

  def load(self, namespace, name, engine_type="cpu"):
    return RollPaillierTensor(self.rp_ctx.load(name, namespace), engine_type)

class RollPaillierTensor(object):
  def __init__(self, roll_pair, engine_type="cpu"):
    self._store = roll_pair
    if engine_type == "cpu":
        self.pub_key, self.prv_key = rpt_engine.keygen()
        # pub_key, prv_key = rpt_engine.keygen()
        self.pub_key = rpt_engine.dump_pub_key(self.pub_key)
        self.prv_key = rpt_engine.dump_prv_key(self.prv_key)
        # self._engine = RptCpuEngine(rpt_engine.dump_pub_key(self.pub_key),
        #                             rpt_engine.dump_prv_key(self.prv_key))
        print("+++++++++++++++++++++1")
    else:
        raise NotImplementedError("todo")

  def put_all(self, data):
      self._store.put_all(data)

  def get_all(self):
      data_list = self._store.get_all()
      print(data_list)
      return b'100'

  def scalar_mul(self, scalar):
    def functor(mat, scalar):
      mat_enc = self._engine.load(mat)
      mat_res_enc = self._engine.scalar_mul(mat_enc, scalar)

      return rpt_engine.dump(mat_res_enc)

    return self._store.map_values(lambda v: functor(v, float(scalar)))

  def obf(self):
      def functor(mat):
          print("[cpu_add]")
          mat_enc = self._engine.load(mat)
          mat_enc_obf = self._engine.obfuscate(mat)
          return self._engine.dump(mat_enc_obf)

      return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

  def add(self, other):
    pub_key = self.pub_key
    prv_key = self.prv_key

    print(prv_key)
    def functor(mat1, mat2):
      _engine = RptCpuEngine(pub_key, prv_key)

      print("[cpu_add]")

      print("+++++++++++++++++++++")

      mat1_enc = _engine.load(mat1)
      mat2_enc = _engine.load(mat2)

      mat_res_enc = _engine.add(mat1_enc, mat2_enc)
      return _engine.dump(mat_res_enc)

    return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: functor(mat1, mat2)))

  def vdot(self, other):
      def functor(mat1, mat2):
          print("[cpu dotmul]")
          mat1_enc = self._engine.load(mat1)
          mat2_enc = self._engine.load(mat2)
          mat2_pln = self._engine.decrypt(mat1_enc)

          mat_res_enc = self._engine.vdot(mat1_enc, mat2_pln)
          return rpt_engine.dump(mat_res_enc)

      return RollPaillierTensor(self._store.join(other._store, lambda left, right : functor(left, right)))

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
          print(left + right)

      partition_result = self._store.join(other._store, lambda left, right : seq_op(left, right))
      # print(partition_result)
      #
      rppr = partition_result
      final_result = rppr.reduce(comb_op)
      return final_result

  def encrypt(self):
      def functor(mat):
          print("[cpu_add]")
          mat_pln = self._engine.load(mat)
          mat_enc = self._engine.encrypt_and_obfuscate(mat)
          return self._engine.dump(mat_enc)

      return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

  def decrypt(self):
      def functor(mat):
          print("[cpu_add]")
          mat_enc = self._engine.load(mat)
          mat_pln = self._engine.decrypt(mat_enc)
          return self._engine.dump(mat_pln)

      return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))


  # def gpu_add(self, other):
  #   def functor(left, right):
  #     pub_key, pvt_key = rpt_engine.gpu_genkey()
  #
  #     enc_mat1 = rpt_engine.gpu_load(left)
  #     enc_mat2 = rpt_engine.gpu_load(right)
  #     pln_mat2 = rpt_engine.gpu_decrypt(enc_mat2, pub_key, pvt_key)
  #
  #     #test [dotmul] [dotadd]
  #     #res = rpt_engine.gpu_dotadd(enc_mat1, enc_mat2, pub_key)
  #     res_mat = rpt_engine.gpu_dotmul(enc_mat1, pln_mat2, pub_key)
  #
  #     res_mat_pln = rpt_engine.gpu_decrypt(res_mat, pub_key, pvt_key)
  #
  #     #test [dump] [load]
  #     h_Byte = rpt_engine.gpu_dump(res_mat_pln)
  #     d_mem = rpt_engine.gpu_load(h_Byte)
  #
  #     res_val = rpt_engine.gpu_decode(d_mem, pub_key)
  #
  #     #mpz_result = rpt_engine.add(gpu_left_mpz, gpu_right_mpz, pub_key, pvt_key)
  #     #return rpt_engine.gpu_dump(mpz_result)
  #     return b'hello'
  #
  #   return self._store.join(other._store, lambda left, right : functor(left, right))
  #   #return 1
  #
  # def gpu_load(self, other):
  #   def functor(left, right):
  #
  #     #load
  #     gpu_left_enc = rpt_engine.gpu_load(left)
  #     gpu_right_enc = rpt_engine.gpu_load(right)
  #
  #     #dump
  #
  #     d_dump = rpt_engine.gpu_dump(gpu_left_enc)
  #     d_load = rpt_engine.gpu_load(d_dump)
  #
  #     #mpz_result = rpt_engine.add(gpu_left_mpz, gpu_right_mpz, pub_key, pvt_key)
  #     #return rpt_engine.gpu_dump(mpz_result)
  #     return b'hello'
  #
  #   return self._store.join(other._store, lambda left, right : functor(left, right))
  #   #return 1
