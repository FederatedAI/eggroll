from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.utils import log_utils
import numpy as np


import rptCPU as CPUEngine
import rptGPU as GPUEngine


class Ciper(object):
    def genkey(self):
        return CPUEngine.keygen()

class Tensor(object):
    def __init__(self):
        pass


#tmpPub, tmpPriv = rptEngine.keygen()

class RptContext(object):
    def __init__(self, rp_ctx: RollPairContext):
        self.rp_ctx = rp_ctx

    def load(self, namespace, name, engine_type="cpu"):
        return RollPaillierTensor(self.rp_ctx.load(namespace, name), engine_type)


class NumpyTensor(Tensor):
    def __init__(self, ndarray, pub, type='cpu'):
        self._pub = pub
        if isinstance(ndarray, int) or isinstance(ndarray, float):
            self._ndarray = np.array([[ndarray]])
        else:
            self._ndarray = ndarray
        self._type = type
        self._engine = CPUEngine
        self.specifyEGN()

    def specifyEGN(self):
        if self._type == "cpu":
            self._engine = CPUEngine
        elif self._type == "gpu":
            self._engine = GPUEngine
        else:
            raise ValueError(self._type)

    def __setstate__(self, state):
        bin_pub, bin_arr, bin_type = state
        self._type = bin_type
        self.specifyEGN()
        self._pub = self._engine.load_pub_key(bin_pub)
        self._ndarray = bin_arr

    def __getstate__(self):
        return self._engine.dump_pub_key(self._pub), self._ndarray, self._type


    def __add__(self, other):
        if isinstance(other, NumpyTensor):
            return NumpyTensor(self._ndarray + other._ndarray, self._pub)
        if isinstance(other, PaillierTensor):
            mng = self._engine.num2Mng(self._ndarray, self._pub)
            return PaillierTensor(self._engine.add(mng, other._store, self._pub), self._pub)

    def __sub__(self, other):
        if isinstance(other, NumpyTensor):
            return NumpyTensor((self._ndarray - other._ndarray), self._pub)
        if isinstance(other, PaillierTensor):
            mng = self._engine.num2Mng(self._ndarray, self._pub)
            sub = self._engine.scalar_mul(other._store, -1, self._pub)
            return PaillierTensor(self._engine.add(mng, sub, self._pub), self._pub)
        if isinstance(other, RollPaillierTensor):
            return RollPaillierTensor(other._store.map_values(lambda v: self - v))

    def __mul__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            return NumpyTensor(self._ndarray * other, self._pub)
        if isinstance(other, NumpyTensor):
            return NumpyTensor(self._ndarray * other._ndarray, self._pub)
        # if isinstance(other, RollPaillierTensor):
        #     return other.mul(self._ndarray)
        if isinstance(other, PaillierTensor):
            mng = self._engine.num2Mng(self._ndarray, self._pub)
            return PaillierTensor(self._engine.vdot(mng, other._store, self._pub), self._pub)

    def __matmul__(self, other):
        if isinstance(other, NumpyTensor):
            return NumpyTensor(self._ndarray.dot(other._ndarray), self._pub)
        if isinstance(other, PaillierTensor):
            mng = self._engine.num2Mng(self._ndarray, self._pub)
            return PaillierTensor(self._engine.matmul(mng, other._store, self._pub), self._pub)

    def T(self):
        return NumpyTensor(self._ndarray.T, self._pub)

    def split(self, num, ax, id):
        a, b = np.split(self._ndarray, (num, ), axis=ax)
        if id == 0:
            return NumpyTensor(a, self._pub)
        else:
            return NumpyTensor(b, self._pub)

    def encrypt(self):
        mng = self._engine.num2Mng(self._ndarray, self._pub)
        return PaillierTensor(self._engine.encrypt_and_obfuscate(mng, self._pub), self._pub)

    def out(self, priv, str = "[CHAN ZHEN NAN]"):
        print(str)
        print(self._ndarray)

class PaillierTensor(Tensor):
    def __init__(self, store, pub, type = "cpu"):
        self._store = store
        self._pub = pub
        self._type = type
        self._engine = CPUEngine
        self.specifyEGN()


    def specifyEGN(self):
        if self._type == "cpu":
            self._engine = CPUEngine
        elif self._type == "gpu":
            self._engine = GPUEngine
        else:
            raise ValueError(self._type)

    def __setstate__(self, state):
        bin_store, bin_pub, bin_type = state
        self._type = bin_type
        self.specifyEGN()
        self._pub = self._engine.load_pub_key(bin_pub)
        self._store = self._engine.load(bin_store)

    def __getstate__(self):
        return self._engine.dump(self._store), self._engine.dump_pub_key(self._pub), self._type

    def __add__(self, other):
        if isinstance(other, NumpyTensor):
            mng = self._engine.num2Mng(other._ndarray, self._pub)
            return PaillierTensor(self._engine.add(self._store, mng, self._pub), self._pub)
        if isinstance(other, PaillierTensor):
            return PaillierTensor(self._engine.add(self._store, other._store, self._pub), self._pub)

    def __sub__(self, other):
        if isinstance(other, NumpyTensor):
            print('bbbbbbbbbbbb')
            sub = self._engine.num2Mng(other._ndarray * (-1), self._pub)
            return PaillierTensor(self._engine.add(self._store, sub, self._pub), self._pub)
        if isinstance(other, PaillierTensor):
            sub = self._engine.scalar_mul(other._store, -1, self._pub)
            return PaillierTensor(self._engine.add(self._store, sub, self._pub), self._pub)

    def __truediv__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            return PaillierTensor(self._engine.scalar_mul(self._store, float(1 / other), self._pub), self._pub)
        if isinstance(other, NumpyTensor):
            mng = self._engine.num2Mng((1 / other._ndarray), self._pub)
            return PaillierTensor(self._engine.mul(self._store, mng, self._pub), self._pub)

        if isinstance(other, PaillierTensor):
            #todo
            print("coming soon")
            return

    def __mul__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            return PaillierTensor(self._engine.scalar_mul(self._store, float(other), self._pub), self._pub)
        if isinstance(other, NumpyTensor):
            mng = self._engine.num2Mng(other._ndarray, self._pub)
            return PaillierTensor(self._engine.vdot(self._store, mng, self._pub), self._pub)
        if isinstance(other, PaillierTensor):
            return PaillierTensor(self._engine.vdot(self._store, other._store, self._pub), self._pub)

    def __matmul__(self, other):
        if isinstance(other, NumpyTensor):
            mng = self._engine.num2Mng(other._ndarray, self._pub)
            return PaillierTensor(self._engine.matmul(self._store, mng, self._pub), self._pub)
        if isinstance(other, PaillierTensor):
            return PaillierTensor(self._engine.matmul(self._store, other._store, self._pub), self._pub)

    def T(self):
        return PaillierTensor(self._engine.transe(self._store), self._pub)

    def mean(self):
        return PaillierTensor(self._engine.mean(self._store, self._pub), self._pub)

    def hstack(self, other):
        if isinstance(other, PaillierTensor):
            return PaillierTensor(self._engine.hstack(self._store, other._store, self._pub), self._pub)

    def encrypt(self):
        if isinstance(self._store, np.ndarray):
            mng = self._engine.num2Mng(self._store, self._pub)
            return PaillierTensor(self._engine.encrypt_and_obfuscate(mng, self._pub), self._pub)

    def decrypt(self, priv):
        return NumpyTensor(self._engine.decryptdecode(self._store, self._pub, priv), self._pub)

    def out(self, priv, str='[LOCAL CZN]'):
        print(str)
        self._engine.print(self._store, self._pub, priv)

class RollPaillierTensor(Tensor):
    def __init__(self, store):
        self._store = store

    def __add__(self, other):
        if isinstance(other, NumpyTensor):
            return RollPaillierTensor(self._store.map_values(lambda v: v + other))
        if isinstance(other, RollPaillierTensor):
            return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: mat1 + mat2))

    def __sub__(self, other):
        if isinstance(other, NumpyTensor):
            print('XXXXXXXXXXXXXXXXXXXX')
            return RollPaillierTensor(self._store.map_values(lambda v: v - other))
        if isinstance(other, RollPaillierTensor):
            return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: mat1 - mat2))

    def __mul__(self, other):
        if isinstance(other, NumpyTensor) or isinstance(other, int) or isinstance(other, float):
            return RollPaillierTensor(self._store.map_values(lambda v: v * other))
        if isinstance(other, RollPaillierTensor):
            return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: mat1 * mat2))

    def __truediv__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            return RollPaillierTensor(self._store.map_values(lambda v: v / other))
        if isinstance(other, NumpyTensor):
            return RollPaillierTensor(self._store.map_values(lambda v: v / other))

    def __matmul__(self, other):
        if isinstance(other, NumpyTensor):
            return RollPaillierTensor(self._store.map_values(lambda v: v @ other))
        if isinstance(other, RollPaillierTensor):
            return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: mat1 @ mat2))

    def mean(self):
        return RollPaillierTensor(self._store.map_values(lambda mat: mat.mean()))

    def T(self):
        return RollPaillierTensor(self._store.map_values(lambda mat: mat.T()))

    def split(self, num, ax):
        a = RollPaillierTensor(self._store.map_values(lambda mat: mat.split(num, ax, 0)))
        b = RollPaillierTensor(self._store.map_values(lambda mat: mat.split(num, ax, 1)))
        return a, b

    def encrypt(self):
        return RollPaillierTensor(self._store.map_values(lambda mat: mat.encrypt()))

    def hstack(self, other):
        if isinstance(other, NumpyTensor):
            return RollPaillierTensor(self._store.map_values(lambda v: v.hstack(other)))
        if isinstance(other, RollPaillierTensor):
            return RollPaillierTensor(self._store.join(other._store, lambda mat1, mat2: mat1.hstack(mat2)))

    #paillier tool
    def decrypt(self, priv):
        def functor(mat, priv):
            _priv = CPUEngine.load_prv_key(priv)
            return mat.decrypt(_priv)
        dump_priv = CPUEngine.dump_prv_key(priv)
        return RollPaillierTensor(self._store.map_values(lambda mat: functor(mat, dump_priv)))

    def out(self, priv, str2 = "[CHAN ZHEN NAN]"):
        def outFunc(mat, priv, str):
            _priv = CPUEngine.load_prv_key(priv)
            mat.out(_priv, str)
        dump_priv = CPUEngine.dump_prv_key(priv)
        return RollPaillierTensor(self._store.map_values(lambda v: outFunc(v, dump_priv, str)))
