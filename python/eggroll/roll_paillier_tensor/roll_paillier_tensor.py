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
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.utils import log_utils
import numpy as np

import roll_paillier_tensor as rpt_engine


log_utils.setDirectory()
LOGGER = log_utils.getLogger()
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
        #print("[__setstate__]++++++++++++++++++++++")
        self.pub_key = rpt_engine.load_pub_key(bin_pub)
        self.priv_key = rpt_engine.load_prv_key(bin_prv)

    def __getstate__(self):
        return rpt_engine.dump_pub_key(self.pub_key), rpt_engine.dump_prv_key(self.priv_key)

    def add(self, x, y):
        return rpt_engine.add(x, y, self.pub_key, self.priv_key)

    def vdot(self, x, y):
        return rpt_engine.vdot(x, y, self.pub_key, self.priv_key)

    def scalar_mul(self, x, scale):
        return rpt_engine.scalar_mul(x, scale, self.pub_key, self.priv_key)

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

    def mean(self, x):
        return rpt_engine.mean(x, self.pub_key, self.priv_key)

    def hstack(self, x, y):
        return rpt_engine.hstack(x, y, self.pub_key, self.priv_key)

    def encrypt(self, x):
        return rpt_engine.encrypt_and_obfuscate(x, self.pub_key)

    def decrypt(self, x):
        return rpt_engine.decrypt(x, self.pub_key, self.priv_key)

    def decode(self, x):
        return rpt_engine.decode(x, self.pub_key, self.priv_key)

    def out(self, x):
        return rpt_engine.print(x, self.pub_key, self.priv_key)

    #interface
    def manager(self, x, y, val):
        return rpt_engine.make_manager(x, y, val, self.pub_key)

    def num2Mng(self, x):
        return rpt_engine.num2Mng(x, self.pub_key)

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
        _engine = self._engine
        def functor(mat, scalar):
            if isinstance(mat, np.ndarray):
                print("scalar:", scalar)
                return mat * scalar
            else:
                print("scalar:+++", scalar)
                mat_enc = _engine.load(mat)
                mat_res = _engine.scalar_mul(mat_enc, scalar)
                return _engine.dump(mat_res)
        return RollPaillierTensor(self._store.map_values(lambda v: functor(v, float(scalar))))

    def obf(self):
        def functor(mat):
            # print("[cpu_add]")
            mat_enc = self._engine.load(mat)
            mat_enc_obf = self._engine.obfuscate(mat)
            return self._engine.dump(mat_enc_obf)

        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

    def add(self, other):
        _engine = self._engine
        def functor(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                return mat1 + mat2

            if isinstance(mat1, np.ndarray):
                m1 = _engine.num2Mng(mat1)
            else:
                m1 = _engine.load(mat1)

            if isinstance(mat2, np.ndarray):
                m2 = _engine.num2Mng(mat2)
            else:
                m2 = _engine.load(mat2)

            mat_res_enc = _engine.add(m1, m2)
            return _engine.dump(mat_res_enc)

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: functor(mat1, mat2)))

    def add_test(self, other):
        _engine = self._engine
        def functor(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                return mat1 + mat2

            if isinstance(mat1, np.ndarray):
                m1 = _engine.num2Mng(mat1)
            else:
                m1 = _engine.load(mat1)

            if isinstance(mat2, np.ndarray):
                m2 = _engine.num2Mng(mat2)
                print("11111111111111111>>>>")
            else:
                m2 = _engine.load(mat2)

            mat_res_enc = _engine.add(m1, m2)
            return _engine.dump(mat_res_enc)

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: functor(mat1, mat2)))

    def vdot(self, other):
        _engine = self._engine
        def functor(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                 return mat1.dot(mat2.T)
            else:
                mat1_enc = _engine.load(mat1)
                mat2_enc = _engine.load(mat2)
                mat2_pln = _engine.decrypt(mat1_enc)
                mat_res_enc = _engine.vdot(mat1_enc, mat2_pln)
                return rpt_engine.dump(mat_res_enc)

        return RollPaillierTensor(self._store.join(other._store, lambda left, right : functor(left, right)))

    def mean(self):
        _engine = self._engine
        def functor(mat):
            if isinstance(mat, np.ndarray):
                print("here~!!!!!!!1111111111")
                return np.array([[mat.mean()]])
            else:
                print("here~!!!!!!!2222222222")
                mat_pln = _engine.load(mat)
                mat_mean = _engine.mean(mat_pln)
                return _engine.dump(mat_mean)

        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))

    def matmul(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                return mat1.dot(mat2)

            if isinstance(mat1, np.ndarray):
                m1 = _engine.num2Mng(mat1)
            else:
                print("bbbbbbbbbbbbbbbb")
                m1 = _engine.load(mat1)

            if isinstance(mat2, np.ndarray):
                print("aaaaaaaaaaaaaaaaa")
                m2 = _engine.num2Mng(mat2)
            else:
                m2 = _engine.load(mat2)

            mat_res_enc = _engine.matmul(m1, m2)
            return(_engine.dump(mat_res_enc))

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2 : seq_op(mat1, mat2)))

    def matmul_r_eql(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                return mat1.dot(mat2)
            if isinstance(mat1, np.ndarray):
                m1 = _engine.num2Mng(mat1)
            else:
                m1 = _engine.load(mat1)

            if isinstance(mat2, np.ndarray):
                m2 = _engine.num2Mng(mat2)
            else:
                m2 = _engine.load(mat2)

            mat_res_enc = _engine.matmul_r_eql(m1, m2)
            return b'100'
            #return (_engine.dump(mat_res_enc))

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2 : seq_op(mat1, mat2)))

    def matmul_c_eql(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                return mat1.dot(mat2)
            if isinstance(mat1, np.ndarray):
                m1 = _engine.num2Mng(mat1)
            else:
                m1 = _engine.load(mat1)

            if isinstance(mat2, np.ndarray):
                m2 = _engine.num2Mng(mat2)
            else:
                m2 = _engine.load(mat2)

            mat_res_enc = _engine.matmul_c_eql(m1, m2)
            return _engine.dump(mat_res_enc)
        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2 : seq_op(mat1, mat2)))

    def hstack(self, other):
        _engine = self._engine
        def seq_op(mat1, mat2):
            if isinstance(mat1, np.ndarray) and isinstance(mat2, np.ndarray):
                return mat1.hstack(mat2)
            if isinstance(mat1, np.ndarray):
                m1 = _engine.num2Mng(mat1)
            else:
                m1 = _engine.load(mat1)

            if isinstance(mat2, np.ndarray):
                m2 = _engine.num2Mng(mat2)
            else:
                m2 = _engine.load(mat2)

            mat_res_enc = _engine.hstack(m1, m2)
            return _engine.dump(mat_res_enc)

        return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2 : seq_op(mat1, mat2)))

    def split(self, num, ax, id):
        _engine = self._engine
        def seq_op(mat, num, ax, id):
            if isinstance(mat, np.ndarray):
                if id == 0:
                    a, b = np.split(mat, (num, ), axis=ax)
                    return a
                else:
                    a, b = np.split(mat, (num, ), axis=ax)
                    return b
            else:
                print("coming soon")
                return b'100'
        # , num, axis, id
        return RollPaillierTensor(self._store.map_values(lambda mat: seq_op(mat, num, ax, id)))

    def encrypt(self):
        _engine = self._engine
        def functor(mat):
            if isinstance(mat, np.ndarray):
                mat_pln = _engine.num2Mng(mat)
                mat_enc = _engine.encrypt(mat_pln)
                return _engine.dump(mat_enc)
            else:
                mat_pln = _engine.load(mat)
                mat_enc = _engine.encrypt(mat_pln)
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
            mat_org = _engine.decode(mat_code)
            return mat_org
        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat)))


    #local function

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

    def matmul_local(self, vec):
        _engine = self._engine
        def seq_op(mat, vec):
            #two numpy mul (l: from disk 2:from memory)
            if isinstance(mat, np.ndarray) and isinstance(vec, np.ndarray):
                res = vec.dot(mat)
                return res
            else:
                #mng mul numpy(l: from disk 2:from memory)
                mat_enc = _engine.load(mat)
                vec_mng = _engine.num2Mng(vec)
                res = _engine.matmul(mat_enc, vec_mng)
                return _engine.dump(res)

        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, vec)))

    def matmul_local_print(self, vec):
        _engine = self._engine
        def seq_op(mat, vec):
            #two numpy mul (l: from disk 2:from memory)
            if isinstance(mat, np.ndarray) and isinstance(vec, np.ndarray):
                # print("mat", mat)
                # print("vec", vec)
                # res = vec.dot(mat)
                return b'10000'
            else:
                #mng mul numpy(l: from disk 2:from memory)
                print("vvvvvvvvvvvvvvv")

                # mat_enc = _engine.load(mat)
                # vec_mng = _engine.num2Mng(vec)
                # res = _engine.matmul(mat_enc, vec_mng)
                return b'1000'

        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, vec)))

    def sub_local(self, vec):
        _engine = self._engine
        def functor(vec1, vec2):
            if isinstance(vec1, np.ndarray) and isinstance(vec2, np.ndarray):
                return vec2 - vec1
            else:
                print("coming soon2222222222222")
                return b'100'

        return RollPaillierTensor(self._store.map_values(lambda v: functor(v, vec)))

    def add_local(self, vec):
        _engine = self._engine
        def functor(vec1, vec2):
            if isinstance(vec1, np.ndarray) and isinstance(vec2, np.ndarray):
                return vec1 + vec2

            if isinstance(vec1, np.ndarray):
                m1 = _engine.num2Mng(vec1)
            else:
                m1 = _engine.load(vec1)

            if isinstance(vec2, np.ndarray):
                m2 = _engine.num2Mng(vec2)
            else:
                m2 = _engine.load(vec2)

            return _engine.dump(_engine.add(m1, m2))

        return RollPaillierTensor(self._store.map_values(lambda v: functor(v, vec)))

    #out
    def out(self):
        _engine = self._engine
        def seq_op(mat, val):
            #two numpy mul (l: from disk 2:from memory)
            if isinstance(mat, np.ndarray):
                print("[out] [numpy] : ", mat)
                return b'100'
            else:
                m = _engine.load(mat)
                mac = _engine.out(m)
                return b'100'
        return RollPaillierTensor(self._store.map_values(lambda v: seq_op(v, float(1))))



