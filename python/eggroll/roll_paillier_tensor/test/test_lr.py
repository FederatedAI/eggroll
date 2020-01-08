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

from eggroll.roll_pair.roll_pair import RollPairContext, RollPair
#from eggroll.roll_paillier_tensor.test.test_roll_paillier_tensor import TestRollPaillierTensor
from eggroll.roll_paillier_tensor.test.rpt_test_assets import get_debug_test_context

import numpy as np
import pandas as pd

store_type = StoreTypes.ROLLPAIR_LEVELDB
max_iter = 20


class TestLR(unittest.TestCase):
    rpc = None
    rptc = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rpc = get_debug_test_context()
        cls.rptc = RptContext(cls.rpc)

    def test_lr(self):
        #base obj
        rpc = TestLR.rpc
        context = TestLR.rptc
        store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

        G = np.array([[0.254879,-1.046633,0.209656,0.074214,-0.441366,-0.377645,-0.485934,0.347072,-0.28757,-0.733474],
                      [-1.142928,-0.781198,-1.166747,-0.923578,0.62823,-1.021418,-1.111867,-0.959523,-0.096672,-0.121683],
                      [-1.451067,-1.406518,-1.456564,-1.092337,-0.708765,-1.168557,-1.305831,-1.745063,-0.499499,-0.302893],
                      [-0.879933,0.420589,-0.877527,-0.780484,-1.037534,-0.48388,-0.555498,-0.768581,0.43396,-0.200928]])

        H = np.array([[0.449512,-1.247226,0.413178,0.303781,-0.123848,-0.184227,-0.219076,0.268537,0.015996,-0.789267,-0.33736,-0.728193,-0.442587,-0.272757,-0.608018,-0.577235,-0.501126,0.143371,-0.466431,-0.554102],
                      [-1.245485,-0.842317,-1.255026,-1.038066,-0.426301,-1.088781,-0.976392,-0.898898,0.983496,0.045702,-0.493639,0.34862,-0.552483,-0.526877,2.253098,-0.82762,-0.780739,-0.376997,-0.310239,0.176301],
                      [-1.549664,-1.126219,-1.546652,-1.216392,-0.354424,-1.167051,-1.114873,-1.26182,-0.327193,0.629755,-0.666881,-0.779358,-0.708418,-0.637545,0.710369,-0.976454,-1.057501,-1.913447,0.795207,-0.149751],
                      [-0.851273,0.733108,-0.843535,-0.786363,-0.049836,-0.424532,-0.509221,-0.679649,0.797298,0.385927,-0.451772,0.453852,-0.431696,-0.494754,-1.182041,0.281228,0.084759,-0.25242,1.038575,0.351054]])


        Y = np.array([[1], [1], [1], [1]])

        rp_x_G = rpc.load('egr', 'rp_x_G')
        rp_x_H = rpc.load('egr', 'rp_x_H')
        rp_x_Y = rpc.load('egr', 'rp_x_Y')


        pub, priv = Ciper().genkey()

        rp_x_G.put('1', NumpyTensor(G, pub))
        rp_x_H.put('1', NumpyTensor(H, pub))
        rp_x_Y.put('1', NumpyTensor(Y, pub))
        X_G = RollPaillierTensor(rp_x_G)
        X_H = RollPaillierTensor(rp_x_H)
        X_Y = RollPaillierTensor(rp_x_Y)

        w_H = NumpyTensor(np.ones((20, 1)), pub)
        w_G = NumpyTensor(np.ones((10, 1)), pub)

        #X_H._store.map_values(lambda v: print("123", v._ndarry))

        learning_rate = 0.15
        itr = 0
        pre_loss_A = None

        while itr < max_iter:
            fw_H1 = X_H @ w_H
            fw_H2 = X_H @ w_H
            enc_fw_H = fw_H1.encrypt()
            enc_fw_H.out(priv, "123")

            enc_fw_square_H = (fw_H1 * fw_H2).encrypt()

            fw_G1 = X_G @ w_G
            fw_G2 = X_G @ w_G
            enc_fw_G = fw_G1.encrypt()
            enc_fw_square_G = (fw_G1 * fw_G2).encrypt()

            enc_agg_wx_G = enc_fw_G + enc_fw_H

            enc_agg_wx_square_G = enc_fw_square_G + enc_fw_square_H + fw_G1 * enc_fw_H * 2

            enc_fore_grad_G = enc_agg_wx_G * 0.25 - X_Y * 0.5

            enc_grad_G = (X_G * enc_fore_grad_G).mean()
            enc_grad_H = (X_H * enc_fore_grad_G).mean()


            enc_grad_G.out(priv, '123')

            grad_A = enc_grad_G.hstack(enc_grad_H)

            learning_rate *= 0.999
            optim_grad_A = grad_A * learning_rate
            optim_grad_G, optim_grad_H = optim_grad_A.decrypt(priv).split(10, 1)

            # w_G.out(priv, "111111111111")
            # optim_grad_G.out(priv, "22222222")

            w_G = w_G - optim_grad_G.T()
            w_H = w_H - optim_grad_H.T()

            enc_half_ywx_G = enc_agg_wx_G * 0.5 * X_Y
            # #todo diversion
            enc_loss_G = (((enc_half_ywx_G * -1)) + enc_agg_wx_square_G / 8 + NumpyTensor(np.log(2), pub)).mean()
            loss_AA = enc_loss_G.decrypt(priv)

            loss_A = next(loss_AA._store.get_all())[1]._ndarray[0][0]
            tmp = 99999 if pre_loss_A is None else loss_A - pre_loss_A
            if pre_loss_A is not None and abs(loss_A - pre_loss_A) < 1e-4:
              break
            pre_loss_A = loss_A
            print("pre_loss_A:", pre_loss_A)

            itr += 1

    def test_lr_local(self):

        G = np.array([[0.254879,-1.046633,0.209656,0.074214,-0.441366,-0.377645,-0.485934,0.347072,-0.28757,-0.733474],
                      [-1.142928,-0.781198,-1.166747,-0.923578,0.62823,-1.021418,-1.111867,-0.959523,-0.096672,-0.121683],
                      [-1.451067,-1.406518,-1.456564,-1.092337,-0.708765,-1.168557,-1.305831,-1.745063,-0.499499,-0.302893],
                      [-0.879933,0.420589,-0.877527,-0.780484,-1.037534,-0.48388,-0.555498,-0.768581,0.43396,-0.200928]])

        H = np.array([[0.449512,-1.247226,0.413178,0.303781,-0.123848,-0.184227,-0.219076,0.268537,0.015996,-0.789267,-0.33736,-0.728193,-0.442587,-0.272757,-0.608018,-0.577235,-0.501126,0.143371,-0.466431,-0.554102],
                      [-1.245485,-0.842317,-1.255026,-1.038066,-0.426301,-1.088781,-0.976392,-0.898898,0.983496,0.045702,-0.493639,0.34862,-0.552483,-0.526877,2.253098,-0.82762,-0.780739,-0.376997,-0.310239,0.176301],
                      [-1.549664,-1.126219,-1.546652,-1.216392,-0.354424,-1.167051,-1.114873,-1.26182,-0.327193,0.629755,-0.666881,-0.779358,-0.708418,-0.637545,0.710369,-0.976454,-1.057501,-1.913447,0.795207,-0.149751],
                      [-0.851273,0.733108,-0.843535,-0.786363,-0.049836,-0.424532,-0.509221,-0.679649,0.797298,0.385927,-0.451772,0.453852,-0.431696,-0.494754,-1.182041,0.281228,0.084759,-0.25242,1.038575,0.351054]])

        Y = np.array([[1], [1], [1], [1]])


        pub_key, priv_key = Ciper().genkey()

        w_H = NumpyTensor(np.ones((20, 1)), pub_key)
        w_G = NumpyTensor(np.ones((10, 1)), pub_key)

        X_G = NumpyTensor(G, pub_key)
        X_H = NumpyTensor(H, pub_key)
        X_Y = NumpyTensor(Y, pub_key)


        learning_rate=0.15
        itr = 0
        pre_loss_A = None
        while itr < max_iter:
            fw_H = X_H @ w_H
            # fw_H_pt = PaillierTensor(fw_H._ndarray, pub_key)
            # enc_fw_H = fw_H_pt.encrypt()
            # fw_square_H = PaillierTensor((fw_H * fw_H)._ndarray, pub_key)
            # enc_fw_square_H = fw_square_H.encrypt()
            #
            # fw_G = X_G @ w_G
            # fw_G_pt = PaillierTensor(fw_G._ndarray, pub_key)
            # enc_fw_G = fw_G_pt.encrypt()
            # fw_square_G = PaillierTensor((fw_G * fw_G)._ndarray, pub_key)
            # enc_fw_square_G = fw_square_G.encrypt()
            #
            # enc_agg_wx_G = enc_fw_G + enc_fw_H
            # # enc_fw_G.out("12312313123")
            #
            # enc_agg_wx_square_G = enc_fw_square_G + enc_fw_square_H + fw_G * enc_fw_H * 2
            #
            # enc_fore_grad_G = enc_agg_wx_G * 0.25 - X_Y * 0.5
            #
            # enc_grad_G = (X_G * enc_fore_grad_G).mean()
            # enc_grad_H = (X_H * enc_fore_grad_G).mean()
            #
            # grad_A = enc_grad_G.hstack(enc_grad_H)
            #
            # learning_rate *= 0.999
            # optim_grad_A = grad_A * learning_rate
            #
            # optim_grad_G, optim_grad_H = optim_grad_A.decrypt(pub_key, priv_key).split(10, 1)
            #
            # w_G = w_G - optim_grad_G.T()
            # w_H = w_H - optim_grad_H.T()
            #
            # enc_half_ywx_G = enc_agg_wx_G * 0.5 * X_Y
            # enc_loss_G = (((enc_half_ywx_G * -1)) + enc_agg_wx_square_G / 8 + NumpyTensor(np.log(2), pub_key)).mean()
            #
            # loss_AA = enc_loss_G.decrypt(pub_key, priv_key)
            #
            # loss_A = loss_AA._ndarray
            # tmp = 99999 if pre_loss_A is None else loss_A - pre_loss_A
            # if pre_loss_A is not None and abs(loss_A - pre_loss_A) < 1e-4:
            #   break
            # pre_loss_A = loss_A
            # print("pre_loss_A:", pre_loss_A)

            itr += 1


if __name__ == '__main__':
    unittest.main()
