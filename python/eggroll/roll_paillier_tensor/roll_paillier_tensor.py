import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Thread

from eggroll.core.aspects import _method_profile_logger
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.constants import StoreTypes
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_pair import create_serdes, create_adapter
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.utils import log_utils
import numpy as np
import os

from eggroll.utils.log_utils import get_logger

L = get_logger()

if os.environ.get("EGGROLL_RPT_ENGINE_MOCK", "0") == "1":
    import eggroll.roll_paillier_tensor.rpt_py_engine as CPUEngine
    import eggroll.roll_paillier_tensor.rpt_py_engine as GPUEngine
else:
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
    def __init__(self, rp_ctx: RollPairContext, options=None):
        if options is None:
            options = {}
        self.rp_ctx = rp_ctx
        self.rp_obf = None

    def set_obfuscator(self, name="asyn_obfuscator"):
        ns = self.rp_ctx.get_session().get_session_id()
        store_opts = {"store_type": StoreTypes.ROLLPAIR_QUEUE}
        self.rp_obf = self.rp_ctx.load(ns, name, store_opts)

    def start_gen_obfuscator(self, pub_key, name="asyn_obfuscator"):
        ns = self.rp_ctx.get_session().get_session_id()
        store_opts = {"store_type": StoreTypes.ROLLPAIR_QUEUE}
        self.rp_obf = self.rp_ctx.load(ns, name, store_opts)

        def func_asyn(partitions):
            part1 = partitions[0]
            serder1 = create_serdes(part1._store_locator._serdes)
            with create_adapter(part1) as db1:
                i = 0
                while True:
                    try:
                        db1.put(serder1.serialize(pub_key.gen_obfuscator()))
                    except InterruptedError as e:
                        L.info(f"finish create asyn obfuscato:{ns}.{name}: {i}")
                        break
                    if i % 10000 == 0:
                        L.debug(f"generating asyn obfuscato:{ns}.{name}: {i}")
                    i += 1
        th = Thread(target=self.rp_obf.with_stores, args=(func_asyn,), daemon=True, name=name)
        # pool.submit(self.rp_obf.with_stores, func_asyn)
        th.start()
        self.rp_ctx.get_session().add_exit_task(self.rp_obf.destroy)

    def load(self, namespace, name, options=None):
        if options is not None:
            options = {}
        # TODO:0: engine_type
        return self.from_roll_pair(self.rp_ctx.load(namespace, name))

    def from_roll_pair(self, rp):
        return RollPaillierTensor(rp, self)


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
            return other.rpt_ctx.from_roll_pair(other._store.map_values(lambda v: self - v))

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

    def encrypt(self, obfs=None):
        mng = self._engine.num2Mng(self._ndarray, self._pub)
        return PaillierTensor(self._engine.encrypt_and_obfuscate(mng, self._pub, obfs=obfs), self._pub)

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
    def __init__(self, store, rpt_ctx):
        self._store = store
        self.rpt_ctx =rpt_ctx

    @_method_profile_logger
    def __add__(self, other):
        if isinstance(other, NumpyTensor):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v + other))
        if isinstance(other, RollPaillierTensor):
            return self.rpt_ctx.from_roll_pair(self._store.join(other._store, lambda mat1, mat2: mat1 + mat2))

    @_method_profile_logger
    def __sub__(self, other):
        if isinstance(other, NumpyTensor):
            print('XXXXXXXXXXXXXXXXXXXX')
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v - other))
        if isinstance(other, RollPaillierTensor):
            return self.rpt_ctx.from_roll_pair(self._store.join(other._store, lambda mat1, mat2: mat1 - mat2))

    @_method_profile_logger
    def __mul__(self, other):
        if isinstance(other, NumpyTensor) or isinstance(other, int) or isinstance(other, float):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v * other))
        if isinstance(other, RollPaillierTensor):
            return self.rpt_ctx.from_roll_pair(self._store.join(other._store, lambda mat1, mat2: mat1 * mat2))
    @_method_profile_logger
    def __rmul__(self, other):
        if isinstance(other, NumpyTensor) or isinstance(other, int) or isinstance(other, float):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v * other))
        if isinstance(other, RollPaillierTensor):
            return self.rpt_ctx.from_roll_pair(self._store.join(other._store, lambda mat1, mat2: mat1 * mat2))

    def __truediv__(self, other):
        if isinstance(other, int) or isinstance(other, float):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v / other))
        if isinstance(other, NumpyTensor):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v / other))

    @_method_profile_logger
    def __matmul__(self, other):
        if isinstance(other, NumpyTensor):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v @ other))
        if isinstance(other, RollPaillierTensor):
            return self.rpt_ctx.from_roll_pair(self._store.join(other._store, lambda mat1, mat2: mat1 @ mat2))

    def mean(self):
        return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda mat: mat.mean()))

    def T(self):
        return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda mat: mat.T()))

    def split(self, num, ax):
        a = self.rpt_ctx.from_roll_pair(self._store.map_values(lambda mat: mat.split(num, ax, 0)))
        b = self.rpt_ctx.from_roll_pair(self._store.map_values(lambda mat: mat.split(num, ax, 1)))
        return a, b

    @_method_profile_logger
    def encrypt(self):
        if self.rpt_ctx.rp_obf is None:
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda mat: mat.encrypt()))
        def func(partitions):
            part_main, part_obf, part_ret = partitions
            serder_main = create_serdes(part_main._store_locator._serdes)
            serder_obf = create_serdes(part_obf._store_locator._serdes)
            serder_ret = create_serdes(part_ret._store_locator._serdes)
            with create_adapter(part_main) as db_main, \
                    create_adapter(part_obf) as db_obf,\
                    create_adapter(part_ret) as db_ret:
                with db_main.iteritems() as rb, db_ret.new_batch() as wb:
                    for k, v in rb:
                        mat = serder_main.deserialize(v)
                        obfs = []
                        L.debug(f"obf qsize: {db_obf.count()}, {part_obf}")
                        for i in range(mat._ndarray.size):
                            obfs.append(serder_obf.deserialize(db_obf.get()))
                        obfs = np.array(obfs).reshape(mat._ndarray.shape)
                        wb.put(k, serder_ret.serialize(serder_main.deserialize(v).encrypt(obfs=obfs)))
        rp_ret = self._store.ctx.load(self._store.get_namespace(), str(uuid.uuid1()))
        self._store.with_stores(func, [self.rpt_ctx.rp_obf, rp_ret])
        return self.rpt_ctx.from_roll_pair(rp_ret)


    def hstack(self, other):
        if isinstance(other, NumpyTensor):
            return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: v.hstack(other)))
        if isinstance(other, RollPaillierTensor):
            return self.rpt_ctx.from_roll_pair(self._store.join(other._store, lambda mat1, mat2: mat1.hstack(mat2)))

    #paillier tool
    def decrypt(self, priv):
        def functor(mat, priv):
            _priv = CPUEngine.load_prv_key(priv)
            return mat.decrypt(_priv)
        dump_priv = CPUEngine.dump_prv_key(priv)
        return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda mat: functor(mat, dump_priv)))

    def out(self, priv, str2 = "[CHAN ZHEN NAN]"):
        def outFunc(mat, priv, str):
            _priv = CPUEngine.load_prv_key(priv)
            mat.out(_priv, str)
        dump_priv = CPUEngine.dump_prv_key(priv)
        return self.rpt_ctx.from_roll_pair(self._store.map_values(lambda v: outFunc(v, dump_priv, str2)))
