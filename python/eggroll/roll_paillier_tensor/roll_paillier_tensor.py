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

from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.utils import log_utils

L = log_utils.get_logger()
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
        #print("[ RptCpuEngine __init__ ]", pub_key, priv_key)
        self.pub_key = pub_key
        self.priv_key = priv_key

    def __setstate__(self, state):
        bin_pub, bin_prv = state
        print("[__setstate__]++++++++++++++++++++++")
        self.pub_key = rpt_engine.load_pub_key(bin_pub)
        self.priv_key = rpt_engine.load_prv_key(bin_prv)

    def __getstate__(self):
        return rpt_engine.dump_pub_key(self.pub_key), rpt_engine.dump_prv_key(self.priv_key)

    def add(self, x, y):
        return rpt_engine.add(x, y, self.pub_key, self.priv_key)

    def vdot(self, x, y):
        return rpt_engine.vdot(x, y, self.pub_key, self.priv_key)

    def scalar_mul(self, x, scale):
        return rpt_engine.add(x, scale, self.pub_key, self.priv_key)

    def matmul(self, x, y):
        return rpt_engine.matmul(x, y, self.pub_key, self.priv_key)

    def matmul_c_eql(self, x, y):
        return rpt_engine.matmul_c_eql(x, y, self.pub_key, self.priv_key)

    def matmul_r_eql(self, x, y):
        return rpt_engine.matmul_r_eql(x, y, self.pub_key, self.priv_key)

    # tool
    def load(self, x):
        return rpt_engine.load(x)

    def dump(self, x):
        return rpt_engine.dump(x)

    def obf(self, x):
        return rpt_engine.obf(x, self.pub_key, self.priv_key)

    def encrypt(self, x):
        return rpt_engine.encrypt(x, self.pub_key, self.priv_key)

    def decrypt(self, x):
        return rpt_engine.decrypt(x, self.pub_key, self.priv_key)

    def decode(self, x):
        return rpt_engine.decode(x, self.pub_key, self.priv_key)

    #interface
    def manager(self, x, y, val):
        return rpt_engine.make_manager(x, y, val, self.pub_key)

class RptLocalTensor(object):
    def __init__(self, row, col, value, pub_key=None, priv_key=None):
        self.pub_key = pub_key
        self.priv_key = priv_key
        self.data = self.localTensor(row, col, value)

    def __setstate__(self, state):
        data = state
        self.data = rpt_engine.load(data)
    def __getstate__(self):
        return rpt_engine.dump(self.data)

    def localTensor(self, row, col, val):
        return rpt_engine.make_manager(row, col, val, self.pub_key)

    def decode(self):
        return rpt_engine.decode(self.data, self.pub_key,self.priv_key)


class RptMixedEngine(RptBaseEngine):
    pass

class RptContext:
    def __init__(self, rp_ctx:RollPairContext):
        self.rp_ctx = rp_ctx

    def load(self, namespace, name, engine_type="cpu"):
        return RollPaillierTensor(self.rp_ctx.load(namespace, name), engine_type)

class RollPaillierTensor(object):
    def __init__(self, roll_pair, engine_type="cpu"):
        self._store = roll_pair
        if engine_type == "cpu":
            self.pub_key, self.prv_key = rpt_engine.keygen()
            # pub_key, prv_key = rpt_engine.keygen()
            # self.pub_key = rpt_engine.dump_pub_key(self.pub_key)
            # self.prv_key = rpt_engine.dump_prv_key(self.prv_key)
            self._engine = RptCpuEngine(self.pub_key, self.prv_key)
        else:
            raise NotImplementedError("todo")

    def put_all(self, data, options={}):
        self._store.put_all(data, options=options)

    def get_all(self):
        data_list = self._store.get_all()
        #print(data_list)
        return b'100'

    def scalar_mul(self, scalar):
        def functor(mat, scalar):
            mat_enc = self._engine.load(mat)
            mat_res_enc = self._engine.scalar_mul(mat_enc, scalar)

            return rpt_engine.dump(mat_res_enc)

        return self._store.map_values(lambda v: functor(v, float(scalar)))

    def obf(self):
        def functor(mat):
            # print("[cpu_add]")
            mat_enc = self._engine.load(mat)
            mat_enc_obf = self._engine.obfuscate(mat)
            return self._engine.dump(mat_enc_obf)

        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

    def add(self, other):
        # pub_key = self.pub_key
        # prv_key = self.prv_key
        _engine = self._engine
        def functor(mat1, mat2):
            mat1_enc = _engine.load(mat1)
            mat2_enc = _engine.load(mat2)

            mat_res_enc = _engine.add(mat1_enc, mat2_enc)
            return _engine.dump(mat_res_enc)

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: functor(mat1, mat2)))

    def vdot(self, other):

        _engine = self._engine
        def functor(mat1, mat2):
            mat1_enc = _engine.load(mat1)
            mat2_enc = _engine.load(mat2)
            mat2_pln = _engine.decrypt(mat1_enc)

            mat_res_enc = _engine.vdot(mat1_enc, mat2_pln)
            return rpt_engine.dump(mat_res_enc)

        return RollPaillierTensor(self._store.join(other._store, lambda left, right : functor(left, right)))

    def matmul(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):
            print("python mat_mul here")
            mat1_enc = _engine.load(mat1)
            mat2_enc = _engine.load(mat2)
            mat2_pln = _engine.decrypt(mat2_enc)

            sub_res = _engine.matmul(mat1_enc, mat2_pln)
            dumping = _engine.dump(sub_res)
            return dumping

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2 : seq_op(mat1, mat2)))

    def matmul_r_eql(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):

            L.info("python mat_mul_r_eql here")
            mat1_enc = _engine.load(mat1)
            mat2_pln = _engine.load(mat2)
            # #mat2_pln = _engine.decrypt(mat2_enc)
            #
            sub_res = _engine.matmul_r_eql(mat1_enc, mat2_pln)
            # dumping = _engine.dump(sub_res)
            return b'asdfafs'

        tmp = self._store.join(other._store, lambda mat1, mat2: seq_op(mat1, mat2))

        return RollPaillierTensor(tmp)

    def matmul_c_eql(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):
            print("python mat_mul_r_eql here")
            mat1_enc = _engine.load(mat1)
            mat2_enc = _engine.load(mat2)
            mat2_pln = _engine.decrypt(mat2_enc)

            sub_res = _engine.matmul_c_eql(mat1_enc, mat2_pln)
            dumping = _engine.dump(sub_res)
            return dumping
        print("step 1")
        tmp = self._store.join(other._store, lambda mat1, mat2: seq_op(mat1, mat2))
        print("step 2")
        return RollPaillierTensor(tmp)

    def encrypt(self):
        _engine = self._engine
        def functor(mat):
            print("[cpu_encrypt]")
            mat_pln = _engine.load(mat)
            mat_enc = _engine.encrypt_and_obfuscate(mat)
            return _engine.dump(mat_enc)

        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

    def decrypt(self):
        _engine = self._engine
        def functor(mat):
           # _engine = RptCpuEngine(pub_key, prv_key)
            mat_enc = _engine.load(mat)
            mat_pln = _engine.decrypt(mat_enc)
            return _engine.dump(mat_pln)
        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

    def decode(self):
        _engine=self._engine
        def functor(mat):
            #_engine = RptCpuEngine(pub_key, prv_key)
            mat_code = _engine.load(mat)
            print('[RptCpuEngine] ', _engine)
            print('dir in rpt cpu engine', dir(_engine))
            mat_org = _engine.decode(mat_code)
            return mat_org
        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))


    #local function
    def matmul_local(self, vec):
        _engine = self._engine
        def seq_op(mat, vec):
            print("python mat_mul here")
            mat_enc = _engine.load(mat)
            vec_mng = _engine.num2Mng(vec)
            res = _engine.matmul(mat_enc, vec_mng)
            return _engine.dump(res)

        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, vec)))

    def matmul_r_eql_local(self, vec):
        _engine = self._engine
        def seq_op(mat, vec):
            mat_enc = _engine.load(mat)
            res = _engine.matmul_r_eql(mat_enc, vec.data)
            return _engine.dump(res)

        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, vec)))

    def matmul_c_eql_local(self, vec):
        _engine = self._engine
        def seq_op(mat, vec):
            print("python mat_mul_r_eql here")
            mat_enc = _engine.load(mat)
            res = _engine.matmul_c_eql(mat_enc, vec.data)
            return _engine.dump(res)

        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, vec)))



    def matmul_local_numpy(self, vec):
        def seq_op(mat, vec):
            # print("matmul_local_numpy : mat", mat)
            # print("matmul_local_numpy : vec", vec)
            res = vec.dot(mat)
            #print("mat row = ", mat.shape[0], "mat col = ", mat.shape[1] )
            #print("vec row = ", vec.shape[0], "vec col = ", vec.shape[1] )
            print("[matmul_local]", res)
            return b'1243'
        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, vec)))
