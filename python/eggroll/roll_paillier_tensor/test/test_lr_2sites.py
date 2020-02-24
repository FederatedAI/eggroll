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
import numpy as np
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptContext, Ciper, NumpyTensor, RollPaillierTensor
from eggroll.roll_paillier_tensor.test.tesst_assets import get_debug_rs_context, guest_options, host_options
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_pair.test.roll_pair_test_assets import get_standalone_context, get_debug_test_context

host_parties = [('host', host_options["self_party_id"])]
guest_parties = [('guest', guest_options["self_party_id"])]
max_iter = 10

class TestLR2SitesBase(unittest.TestCase):
    rpc = None
    rptc = None

    def setUp(self) -> None:
        self.epoch = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rpc = get_debug_test_context(True)
        cls.rsc_guest = get_debug_rs_context(cls.rpc, "rs_sid1", guest_options)
        cls.rsc_host = get_debug_rs_context(cls.rpc, "rs_sid1", host_options)
        cls.rptc = RptContext(cls.rpc)

    @classmethod
    def tearDown(cls) -> None:
        cls.rpc.get_session().stop()

    def testLRGuest(self):
        rp_ctx = self.rpc
        rs_ctx = self.rsc_guest
        _tag = "Hello2"

        pub, priv = Ciper().genkey()
        self.rptc.start_gen_obfuscator(pub_key=pub)
        rs_ctx.load(name="roll_pair_name.test_key_pair", tag="pub_priv_key").push((pub,priv), host_parties)
        G = np.array(
            [[0.254879, -1.046633, 0.209656, 0.074214, -0.441366, -0.377645, -0.485934, 0.347072, -0.28757, -0.733474],
             [-1.142928, -0.781198, -1.166747, -0.923578, 0.62823, -1.021418, -1.111867, -0.959523, -0.096672,
              -0.121683],
             [-1.451067, -1.406518, -1.456564, -1.092337, -0.708765, -1.168557, -1.305831, -1.745063, -0.499499,
              -0.302893],
             [-0.879933, 0.420589, -0.877527, -0.780484, -1.037534, -0.48388, -0.555498, -0.768581, 0.43396,
              -0.200928]])

        Y = np.array([[1], [1], [1], [1]])
        w_G = NumpyTensor(np.ones((10, 1)), pub)

        #
        rp_x_G = rp_ctx.load('namespace', 'G')
        rp_x_Y = rp_ctx.load('namespace', 'Y')
        rp_w_G = rp_ctx.load('namespace', 'w_G')

        rp_x_G.put('1', NumpyTensor(G, pub))
        rp_x_Y.put('1', NumpyTensor(Y, pub))
        rp_w_G.put('1', w_G)

        X_G = self.rptc.from_roll_pair(rp_x_G)
        Y = self.rptc.from_roll_pair(rp_x_Y)
        itr = 0
        learning_rate = 0.15
        while itr < max_iter:
            self.epoch = str(itr)
            enc_fw_H = self._guest_pull("enc_fw_H")
            enc_fw_square_H = self._guest_pull("enc_fw_square_H")

            fw_G = X_G @ w_G
            enc_fw_G = fw_G.encrypt()
            enc_fw_square_G = (fw_G * fw_G).encrypt()

            enc_agg_wx_G = enc_fw_H + enc_fw_G
            enc_agg_wx_square_G = enc_fw_square_G + enc_fw_square_H + fw_G * enc_fw_H * 2

            enc_fore_grad_G = 0.25 * enc_agg_wx_G - 0.5 * Y
            self._guest_push(enc_fore_grad_G, "enc_fore_grad_G")

            enc_grad_G = (X_G * enc_fore_grad_G).mean()
            enc_grad_H = self._guest_pull("enc_grad_H")
            grad_A = enc_grad_G.hstack(enc_grad_H)
            learning_rate *= 0.999
            optim_grad_A = grad_A * learning_rate
            optim_grad_G, optim_grad_H = optim_grad_A.decrypt(priv).split(10, 1)
            self._guest_push(optim_grad_H, "optim_grad_H")
            w_G = w_G - optim_grad_G.T()

            enc_half_ywx_G = enc_agg_wx_G * 0.5 * Y

            enc_loss_G = ((-1 * enc_half_ywx_G ) + enc_agg_wx_square_G / 8 + NumpyTensor(np.log(2), pub)).mean()
            loss_AA = enc_loss_G.decrypt(priv)
            loss_A = next(loss_AA._store.get_all())[1]._ndarray[0][0]
            pre_loss_A = loss_A
            print("pre_loss_A:", pre_loss_A)
            itr += 1

    def _push(self, rsc, obj, tag, parties):
        if self.epoch is not None:
            tag = tag + "." + str(self.epoch)
        if isinstance(obj, RollPaillierTensor):
            obj = obj._store
        return rsc.load(name="roll_pair_name.table", tag=tag).push(obj, parties)

    def _pull(self, rsc, tag, parities):
        if self.epoch is not None:
            tag = tag + "." + str(self.epoch)
        futures = rsc.load(name="roll_pair_name.table", tag=tag).pull(parities)
        ret = []
        for f in futures:
            obj = f.result()
            if isinstance(obj, RollPair):
                ret.append(self.rptc.from_roll_pair(obj))
            else:
                ret.append(obj)
        return ret

    def _host_push(self, obj, tag):
        self._push(self.rsc_host, obj, tag, guest_parties)

    def _host_pull(self, tag):
        return self._pull(self.rsc_host, tag, guest_parties)[0]

    def _guest_push(self, obj, tag):
        self._push(self.rsc_guest, obj, tag, host_parties)

    def _guest_pull(self, tag):
        return self._pull(self.rsc_guest, tag, host_parties)[0]

    def testLRHost(self):
        #  #multi context
        rpt_ctx = self.rptc
        rp_ctx = self.rpc
        rs_ctx = self.rsc_host
        _tag = "Hello2"


        #local RP
        H = np.array([[0.449512,-1.247226,0.413178,0.303781,-0.123848,-0.184227,-0.219076,0.268537,0.015996,-0.789267,-0.33736,-0.728193,-0.442587,-0.272757,-0.608018,-0.577235,-0.501126,0.143371,-0.466431,-0.554102],
                      [-1.245485,-0.842317,-1.255026,-1.038066,-0.426301,-1.088781,-0.976392,-0.898898,0.983496,0.045702,-0.493639,0.34862,-0.552483,-0.526877,2.253098,-0.82762,-0.780739,-0.376997,-0.310239,0.176301],
                      [-1.549664,-1.126219,-1.546652,-1.216392,-0.354424,-1.167051,-1.114873,-1.26182,-0.327193,0.629755,-0.666881,-0.779358,-0.708418,-0.637545,0.710369,-0.976454,-1.057501,-1.913447,0.795207,-0.149751],
                      [-0.851273,0.733108,-0.843535,-0.786363,-0.049836,-0.424532,-0.509221,-0.679649,0.797298,0.385927,-0.451772,0.453852,-0.431696,-0.494754,-1.182041,0.281228,0.084759,-0.25242,1.038575,0.351054]])


        rp_x_H = rp_ctx.load('namespace', 'H')

        # pub, priv = Ciper().genkey()
        pub, priv = rs_ctx.load(name="roll_pair_name.test_key_pair", tag="pub_priv_key").pull(guest_parties)[0].result()
        rpt_ctx.start_gen_obfuscator(pub_key=pub)
        rp_x_H.put('1', NumpyTensor(H, pub))
        X_H = self.rptc.from_roll_pair(rp_x_H)
        w_H = NumpyTensor(np.ones((20, 1)), pub)
        itr = 0
        while itr < max_iter:
            self.epoch = itr
            fw_H = X_H @ w_H
            enc_fw_H = fw_H.encrypt()
            enc_fw_square_H = (fw_H * fw_H).encrypt()
            self._host_push(enc_fw_H, "enc_fw_H")
            self._host_push(enc_fw_square_H, "enc_fw_square_H")
            enc_fore_grad_G = self._host_pull("enc_fore_grad_G")
            enc_grad_H = (X_H * enc_fore_grad_G).mean()
            self._host_push(enc_grad_H, "enc_grad_H")
            optim_grad_H = self._host_pull("optim_grad_H")
            w_H = w_H - optim_grad_H.T()
            itr += 1


class TestLR2SitesStandalone(TestLR2SitesBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rpc = get_standalone_context()
        cls.rsc_guest = get_debug_rs_context(cls.rpc, "rs_sid1", guest_options)
        cls.rsc_host = get_debug_rs_context(cls.rpc, "rs_sid1", host_options)