import grpc
import time
import unittest
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.constants import StoreTypes

##### RollPaillierTensor
from eggroll.core.session import ErSession
from eggroll.roll_paillier_tensor.roll_paillier_tensor import NumpyTensor
from eggroll.roll_paillier_tensor.roll_paillier_tensor import PaillierTensor
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RollPaillierTensor
from eggroll.roll_paillier_tensor.roll_paillier_tensor import Ciper
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptContext


##### RollSite
from eggroll.roll_paillier_tensor.test.tesst_assets import get_debug_rs_context, host_options
from eggroll.roll_pair.roll_pair import RollPairContext, RollPair
from eggroll.roll_site.roll_site import RollSiteContext
from eggroll.roll_pair.test.roll_pair_test_assets import get_debug_test_context, \
    get_cluster_context, get_standalone_context, get_default_options

##### Tool
import numpy as np
import pandas as pd


store_type = StoreTypes.ROLLPAIR_LEVELDB
max_iter = 10

is_standalone = True
manager_port_guest = 5690
egg_port_guest = 20001
transfer_port_guest = 20002
manager_port_host = 5690
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


class TestLR_host(unittest.TestCase):
    rpc = None
    rptc = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rpc = get_debug_test_context(True)
        cls.rptc = RptContext(cls.rpc)
        cls.rsc = get_debug_rs_context(cls.rpc, "rs_sid1", host_options)

    def testLRHost(self):
        #  #multi context
        rpt_ctx = self.rptc
        rp_ctx = self.rpc
        rs_ctx = self.rsc
        _tag = "Hello2"
        #rs = rs_ctx.load(name="roll_pair_h2g.table", tag="{}".format(_tag))

        rpt_store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

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
        w_G = NumpyTensor(np.ones((10, 1)), pub)

        learning_rate = 0.15
        itr = 0
        pre_loss_A = None
        while itr < max_iter:
            round = str(itr)
            fw_H1 = X_H @ w_H
            fw_H2 = X_H @ w_H
            enc_fw_H = fw_H1.encrypt()

            enc_fw_square_H = (fw_H1 * fw_H2).encrypt()

            #get fw_G1
            rs = rs_ctx.load(name="roll_pair_name.table", tag="fw_G1" + round)
            fw_G1 = rs.pull(guest_parties)[0].result()

            rs = rs_ctx.load(name="roll_pair_name.table", tag="enc_fw_G" + round)
            enc_fw_G = rs.pull(guest_parties)[0].result()

            #get enc_fw_sqre_G
            rs = rs_ctx.load(name="roll_pair_name.table", tag="enc_fw_square_G" +round)
            enc_fw_square_G = rs.pull(guest_parties)[0].result()

            enc_agg_wx_G = enc_fw_H + self.rptc.from_roll_pair(enc_fw_G)

            enc_agg_wx_square_G = self.rptc.from_roll_pair(enc_fw_square_G) + enc_fw_square_H + self.rptc.from_roll_pair(fw_G1) * enc_fw_H * 2

            #get X_Y X_G
            rs = rs_ctx.load(name="roll_pair_name.table", tag="X_Y" + round)
            X_Y = rs.pull(guest_parties)[0].result()

            rs = rs_ctx.load(name="roll_pair_name.table", tag="X_G" + round)
            X_G = rs.pull(guest_parties)[0].result()

            # rs = rs_ctx.load(name="roll_pair_name.table", tag="W_G" + round)
            # w_G = rs.pull(guest_parties)[0].result()

            enc_fore_grad_G = 0.25 * enc_agg_wx_G - 0.5 * self.rptc.from_roll_pair(X_Y)

            enc_grad_G = (self.rptc.from_roll_pair(X_G) * enc_fore_grad_G).mean()
            enc_grad_H = (X_H * enc_fore_grad_G).mean()

            grad_A = enc_grad_G.hstack(enc_grad_H)

            learning_rate *= 0.999
            optim_grad_A = grad_A * learning_rate
            optim_grad_G, optim_grad_H = optim_grad_A.decrypt(priv).split(10, 1)

            # w_G = RollPaillierTensor(w_G) - optim_grad_G.T()
            w_G = w_G - optim_grad_G.T()
            w_H = w_H - optim_grad_H.T()

            #send w_G
            rs = rs_ctx.load(name="roll_pair_name.table", tag="W_G_result" + round)
            future = rs.push(w_G._store, guest_parties)
            print("W_G_result1 send")

            enc_half_ywx_G = enc_agg_wx_G * 0.5 * self.rptc.from_roll_pair(X_Y)

            enc_loss_G = (((-1 * enc_half_ywx_G )) + enc_agg_wx_square_G / 8 + NumpyTensor(np.log(2), pub)).mean()
            loss_AA = enc_loss_G.decrypt(priv)

            loss_A = next(loss_AA._store.get_all())[1]._ndarray[0][0]
            tmp = 99999 if pre_loss_A is None else loss_A - pre_loss_A
            pre_loss_A = loss_A
            print("pre_loss_A:", pre_loss_A)



    #     # fw_G1 = X_G @ w_G
    #     # fw_G2 = X_G @ w_G
    #     # enc_fw_G = fw_G1.encrypt()
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
