import grpc
import time
import unittest
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.constants import StoreTypes

from eggroll.core.session import ErSession
from eggroll.roll_paillier_tensor.roll_paillier_tensor import NumpyTensor
from eggroll.roll_paillier_tensor.roll_paillier_tensor import PaillierTensor
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RollPaillierTensor
from eggroll.roll_paillier_tensor.roll_paillier_tensor import Ciper
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptContext

# ##### RollSite
from eggroll.roll_paillier_tensor.test.tesst_assets import get_debug_rs_context, guest_options
from eggroll.roll_pair.roll_pair import RollPairContext, RollPair
from eggroll.roll_site.roll_site import RollSiteContext
from eggroll.roll_pair.test.roll_pair_test_assets import get_debug_test_context, \
    get_cluster_context, get_standalone_context, get_default_options

import numpy as np
import pandas as pd

store_type = StoreTypes.ROLLPAIR_LEVELDB
max_iter = 10
#
is_standalone = True
manager_port_guest = 5690
egg_port_guest = 20001
transfer_port_guest = 20002
manager_port_host = 5691
egg_port_host = 20001
transfer_port_host = 20002
host_parties = [('host', '10001')]
guest_parties = [('guest', '10002')]

host_partyId = 10001
host_ip = 'localhost'
host_rs_port = 9395
guest_partyId = 10002
guest_ip = 'localhost'
guest_rs_port = 9396


class TestLR_guest(unittest.TestCase):
    rpc = None
    rptc = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rpc = get_debug_test_context(True)
        cls.rsc = get_debug_rs_context(cls.rpc, "rs_sid1", guest_options)
        cls.rptc = RptContext(cls.rpc)

    @classmethod
    def tearDown(cls) -> None:
        cls.rpc.get_session().stop()

    def testLRGuest(self):
        #base obj
        rpc = TestLR_guest.rpc
        rpt_store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

        rpt_ctx = self.rptc
        rp_ctx = self.rpc
        rs_ctx = self.rsc
        _tag = "Hello2"
        #rs = rs_ctx.load(name="roll_pair_h2g.table", tag="{}".format(_tag))
        rpt_store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

        # #local RP
        pub, priv = Ciper().genkey()
        # rpt_ctx.start_gen_obfuscator(pub_key=pub)
        rs_ctx.load(name="roll_pair_name.test_key_pair", tag="pub_priv_key").push((pub,priv), host_parties) #[0].result()
        #base data
        G = np.array([[0.254879,-1.046633,0.209656,0.074214,-0.441366,-0.377645,-0.485934,0.347072,-0.28757,-0.733474],
                      [-1.142928,-0.781198,-1.166747,-0.923578,0.62823,-1.021418,-1.111867,-0.959523,-0.096672,-0.121683],
                      [-1.451067,-1.406518,-1.456564,-1.092337,-0.708765,-1.168557,-1.305831,-1.745063,-0.499499,-0.302893],
                      [-0.879933,0.420589,-0.877527,-0.780484,-1.037534,-0.48388,-0.555498,-0.768581,0.43396,-0.200928]])

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
        # fw_G1 = X_G @ w_G
        # enc_fw_square_G = (fw_G1 * fw_G1).encrypt()
        # enc_fw_square_G.out(priv,"aa334")
        # return
        X_Y = self.rptc.from_roll_pair(rp_x_Y)

        learning_rate = 0.15
        itr = 0
        pre_loss_A = None

        #round 1

        while itr < max_iter:
            round = str(itr)
            # X_G = NumpyTensor(G, pub)
            fw_G1 = X_G @ w_G
            fw_G2 = X_G @ w_G

            enc_fw_G = fw_G1.encrypt()
            enc_fw_square_G = (fw_G1 * fw_G2).encrypt()

            rs = rs_ctx.load(name="roll_pair_name.table", tag="fw_G1" + round)
            futures = rs.push(fw_G1._store, host_parties)

            rs = rs_ctx.load(name="roll_pair_name.table", tag="enc_fw_G" + round)
            futures = rs.push(enc_fw_G._store, host_parties)

            rs = rs_ctx.load(name="roll_pair_name.table", tag="enc_fw_square_G" + round)
            futures = rs.push(enc_fw_square_G._store, host_parties)

            rs = rs_ctx.load(name="roll_pair_name.table", tag="X_Y" + round)
            futures = rs.push(rp_x_Y, host_parties)


            rs = rs_ctx.load(name="roll_pair_name.table", tag="X_G" + round)
            futures = rs.push(rp_x_G, host_parties)

            # rs = rs_ctx.load(name="roll_pair_name.table", tag="W_G" + round)
            # futures = rs.push(rp_w_G, host_parties)


            # time.sleep(5)
            # print("sleep 5 sec")
            rs = rs_ctx.load(name="roll_pair_name.table", tag="W_G_result" + round)
            w_G = self.rptc.from_roll_pair(rs.pull(host_parties)[0].result())

    #     # enc_fw_square_G = (fw_G1 * fw_G2).encrypt()
        #     #
        #     # enc_agg_wx_G = enc_fw_G + enc_fw_H
        #     #
        #     # enc_agg_wx_square_G = enc_fw_square_G + enc_fw_square_H + fw_G1 * enc_fw_H * 2
        #     #
        #     # enc_fore_grad_G = enc_agg_wx_G * 0.25 - X_Y * 0.5
        #     #
        #     # enc_grad_G = (X_G * enc_fore_grad_G).mean()
        #     # enc_grad_H = (X_H * enc_fore_grad_G).mean()
        #     #
        #     #
        #     # enc_grad_G.out(priv, '123')
        #     #
        #     # grad_A = enc_grad_G.hstack(enc_grad_H)
        #     #
        #     # learning_rate *= 0.999
        #     # optim_grad_A = grad_A * learning_rate
        #     # optim_grad_G, optim_grad_H = optim_grad_A.decrypt(priv).split(10, 1)
        #     #
        #     # # w_G.out(priv, "111111111111")
        #     # # optim_grad_G.out(priv, "22222222")
        #     #
        #     # w_G = w_G - optim_grad_G.T()
        #     # w_H = w_H - optim_grad_H.T()
        #     #
        #     # enc_half_ywx_G = enc_agg_wx_G * 0.5 * X_Y
        #     # # #todo diversion
        #     # enc_loss_G = (((enc_half_ywx_G * -1)) + enc_agg_wx_square_G / 8 + NumpyTensor(np.log(2), pub)).mean()
        #     # loss_AA = enc_loss_G.decrypt(priv)
        #     #
        #     # loss_A = next(loss_AA._store.get_all())[1]._ndarray[0][0]
        #     # tmp = 99999 if pre_loss_A is None else loss_A - pre_loss_A
        #     # if pre_loss_A is not None and abs(loss_A - pre_loss_A) < 1e-4:
        #     #   break
        #     # pre_loss_A = loss_A
        #     # print("pre_loss_A:", pre_loss_A)
        #
            itr += 1



if __name__ == '__main__':
    unittest.main()
