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
from eggroll.roll_paillier_tensor.test.test_assets import get_debug_rs_context, guest_options, host_options

from eggroll.roll_paillier_tensor.test.test_assets import test_data_dir
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_pair.test.roll_pair_test_assets import get_standalone_context, get_debug_test_context
from datetime import datetime
import os
import time

host_parties = [('host', host_options["self_party_id"])]
guest_parties = [('guest', guest_options["self_party_id"])]
max_iter = 3


class TestLR2SitesBase(unittest.TestCase):
    def init_job(self):
        self.epoch = None
        self.job_id = None
        self.job_id_path = test_data_dir + "rpt_job_id.txt"
        if os.path.exists(self.job_id_path):
            with open(test_data_dir + "rpt_job_id.txt") as job_id_fd:
                self.job_id = job_id_fd.readline()
        else:
            self.job_id = "rpt_job_" + datetime.now().strftime('%Y%m%d.%H%M%S.%f')
            with open(test_data_dir + "rpt_job_id.txt", "w") as job_id_fd:
                job_id_fd.write(self.job_id)
        print("rpt_job_id:", self.job_id)

    def setUp(self) -> None:
        self.init_job()
        self.rpc = get_debug_test_context(True)
        self.rsc_guest = get_debug_rs_context(self.rpc, self.job_id, guest_options)
        self.rsc_host = get_debug_rs_context(self.rpc, self.job_id, host_options)
        self.rptc = RptContext(self.rpc)

    def tearDown(self) -> None:
        self.rpc.get_session().stop()
        if os.path.exists(self.job_id_path):
            os.remove(self.job_id_path)

    def testLRGuest(self):
        rp_ctx = self.rpc
        rs_ctx = self.rsc_guest

        pub, priv = Ciper().genkey()
        self.rptc.start_gen_obfuscator(pub_key=pub)
        rs_ctx.load(name="roll_pair_name.test_key_pair", tag="pub_priv_key").push((pub, priv), host_parties)
        data = np.loadtxt(test_data_dir + "breast_b_10000.csv", delimiter=",", skiprows=1)
        G = data[:, 2:]
        Y = data[:, 1:2]
        w_G = NumpyTensor(np.ones((10, 1)), pub)

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

    def testLRHost(self):
        rpt_ctx = self.rptc
        rp_ctx = self.rpc
        rs_ctx = self.rsc_host
        data = np.loadtxt(test_data_dir + "breast_a_10000.csv", delimiter=",", skiprows=1)
        H = data[:, 1:]
        rp_x_H = rp_ctx.load(self.job_id, 'H')
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


class TestLR2SitesStandalone(TestLR2SitesBase):
    def setUp(self) -> None:
        self.init_job()
        self.rpc = get_standalone_context()
        self.rsc_guest = get_debug_rs_context(self.rpc, self.job_id, guest_options)
        self.rsc_host = get_debug_rs_context(self.rpc, self.job_id, host_options)
        self.rptc = RptContext(self.rpc)