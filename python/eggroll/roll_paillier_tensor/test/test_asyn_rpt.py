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
import unittest
from concurrent.futures.thread import ThreadPoolExecutor

from collections import defaultdict
import random

import time

from eggroll.core.constants import StoreTypes
from eggroll.roll_paillier_tensor import rpt_py_engine
from eggroll.roll_paillier_tensor.roll_paillier_tensor import NumpyTensor, PaillierTensor
from eggroll.roll_paillier_tensor.rpt_py_engine import AsyncPaillierPublicKey, encrypt_and_obfuscate, decryptdecode
from eggroll.roll_pair import *
from eggroll.roll_pair.test.roll_pair_test_assets import get_debug_test_context
from federatedml.secureprotol.fate_paillier import PaillierKeypair
import numpy as np

class TestAsynRpt(unittest.TestCase):
    def setUp(self):
        self.ctx = get_debug_test_context()

    def tearDown(self) -> None:
        print("stop test session")
        self.ctx.get_session().stop()

    def testWithStores(self):
        # store_opts2 = {"store_type": StoreTypes.ROLLPAIR_QUEUE, "capacity": 100}
        store_opts2 = {"store_type": StoreTypes.ROLLPAIR_QUEUE}
        rp = self.ctx.load("ns1", "n1")
        rp_q = self.ctx.load("ns1", "n2", store_opts2)
        rp.put_all([("k1", "v1"), ("k2", "v2")])

        def func_asyn(partitions):
            import time
            part1 = partitions[0]
            serder1 = create_serdes(part1._store_locator._serdes)
            with create_adapter(part1) as db1:
                for i in range(100):
                    db1.put(serder1.serialize("a" + str(i)))
                    time.sleep(0.1)

        def func_syn(partitions):
            part1, part2 = partitions
            serder1 = create_serdes(part1._store_locator._serdes)
            serder2 = create_serdes(part2._store_locator._serdes)
            with create_adapter(part1) as db1, create_adapter(part2) as db2:
                for i in range(100):
                    db1.put(serder1.serialize("q" + str(i)), db2.get())
        pool = ThreadPoolExecutor()
        pool.submit(rp_q.with_stores, func_asyn)

        rp.with_stores(func_syn, [rp_q])
        print(list(rp.get_all()))
        pool.shutdown()

    def testNpTensor(self):
        pub, priv = PaillierKeypair().generate_keypair()
        pub2 = AsyncPaillierPublicKey(pub)
        np1 = np.array([[ -5.457078],
                        [ -7.832643],
                        [-14.418312],
                        [ -2.831291]])
        np2 = np.array([[  6.18417921],
                        [ 44.85495244],
                        [124.03486276],
                        [ 22.37115939]])
        np1 = np2
        na1 = NumpyTensor(np1, pub2)
        obfs = []
        for i in range(np1.size):
            obfs.append(pub2.gen_obfuscator())
        obfs = np.array(obfs).reshape(np1.shape)
        ea1 = encrypt_and_obfuscate(np1, pub2, obfs=obfs)
        print("aa23",decryptdecode(ea1,pub2,priv))
        pa1 = na1.encrypt()
        pa1.decrypt(priv).out(priv,"aa22")


    def testSecProtocol(self):
        pub, priv = PaillierKeypair().generate_keypair()
        pub2 = AsyncPaillierPublicKey(pub)
        stat = defaultdict(float)
        for i in range(1000):
            # r = 0 is not compatible
            r = i + 1
            value = random.uniform(-1 * 2 << 40, 2 << 40)
            start = time.time()
            expected = pub.encrypt(value, random_value=r).ciphertext(False)
            stat["old_encrypt"] += time.time() - start
            start = time.time()
            actual = pub2.encrypt(value, random_value=r).ciphertext(False)
            stat["new_encrypt"] += time.time() - start

            self.assertEqual(expected, actual)
            start = time.time()
            encoding = pub2.encode(value).encoding
            stat["new_encrypt:encode"] += time.time() - start
            start = time.time()
            cipher_text = pub2.raw_encrypt(encoding)
            stat["new_encrypt:raw_encrypt"] += time.time() - start
            start = time.time()
            obf = pub2.gen_obfuscator(r)
            stat["new_encrypt:gen_obfuscator"] += time.time() - start
            start = time.time()
            actual = pub2.apply_obfuscator(cipher_text, obf=obf)
            stat["new_encrypt:apply_obfuscator"] += time.time() - start
            self.assertEqual(expected, actual)
        print("======stat_time=====")
        for k, v in stat.items():
            print(f"{k}\t{v}")

    def testPyEngine(self):
        pub, priv = rpt_py_engine.keygen()
        t1 = NumpyTensor(np.array([[1, 2], [3, 4], [5, 6]]), pub)
        t2 = NumpyTensor(np.array([[0, 2], [3, 4], [5, 6]]), pub)
        tp1 = PaillierTensor(np.array([[0, 2], [3, 4], [5, 6]]), pub)
        add_expected = np.array([[1,  4],
                                [6,  8],
                                [10, 12]])
        t3 = t1 + t2
        print(t3._ndarray)
        self.assertListEqual(add_expected.tolist(), t3._ndarray.tolist())
        t4 = t1.encrypt() + tp1
        print(t4.decrypt(priv)._ndarray, t4._store)
        self.assertListEqual(add_expected.tolist(), t4.decrypt(priv)._ndarray.tolist())

    def testPyEnginePerformance(self):
        pub, priv = rpt_py_engine.keygen()
        np1 = np.ones((10000, 1))
        np2 = np.ones((10000, 10))
        tp1 = PaillierTensor(np1, pub)
        nt2 = NumpyTensor(np2, pub)
        start = time.time()
        tp1 = tp1.encrypt()
        print("t1", time.time() - start)
        start = time.time()
        nt2 * tp1
        print("t2", time.time() - start)
        start = time.time()
        nt2 + tp1
        print("t3", time.time() - start)


