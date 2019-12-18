import grpc
import time
import unittest
import roll_paillier_tensor as rpt_engine
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RollPaillierTensor as rpt, RollPaillierTensor
from eggroll.core.constants import StoreTypes

from eggroll.core.session import ErSession
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptContext
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptLocalTensor
from eggroll.roll_pair.roll_pair import RollPairContext, RollPair
from eggroll.roll_paillier_tensor.test.test_roll_paillier_tensor import TestRollPaillierTensor

import numpy as np

store_type = StoreTypes.ROLLPAIR_LEVELDB
max_iter = 1


class TestLR(unittest.TestCase):
  def test_lr(self):
    #base obj
    session = ErSession(session_id="zhanan", options={"eggroll.deploy.mode": "standalone"})
    context = RptContext(RollPairContext(session))
    store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

    #base elem
    X_H_enc = context.load("ns", "mat_a", "cpu")
    X_G_enc = context.load("ns", "mat_b", "cpu")
    X_Y_enc = context.load("ns", "mat_y", "cpu")

    X_H_dco = X_H_enc.decrypt()
    X_G_dco = X_G_enc.decrypt()
    X_Y_dco = X_Y_enc.decrypt()
    #
    # # remote
    X_H = X_H_dco.decode()
    X_G = X_G_dco.decode()
    X_Y = X_Y_dco.decode()

    w_H_dco = RptLocalTensor(20, 1, 1, X_G_enc.pub_key, X_G_enc.prv_key)
    w_G_dco = RptLocalTensor(10, 1, 1, X_H_enc.pub_key, X_H_enc.prv_key)

    #local
    w_H = w_H_dco.decode()
    w_G = w_G_dco.decode()

    print("w_H", w_H)
    print("w_G", w_G)

    learning_rate=0.15

    fw_H = X_H.matmul_local(w_H)
    fw_G = X_G.matmul_local(w_G)

    enc_fw_H = fw_H.encrypt()
    enc_fw_H2 = (fw_H.vdot(fw_H)).encrypt()
    enc_fw_G = fw_G.encrypt()
    enc_fw_G2 = (fw_G.vdot(fw_G)).encrypt()

    #154
    enc_agg_wx_G = enc_fw_G.add(enc_fw_H)

    enc_tmp = enc_fw_H.matmul(fw_G)
    enc_last = enc_tmp.scalar_mul(2)
    squre2 = enc_fw_H2.add(enc_fw_G2)
    enc_agg_wx_square_G = squre2.add(enc_last)

    # #163
    tmp_l = enc_agg_wx_G.scalar_mul(0.25)
    tmp_r = X_Y.scalar_mul(-0.5)
    enc_fore_grad_G = tmp_l.add_test(tmp_r)
    #
    #tmp_1 = enc_fore_grad_G.matmul_local_print(X_G)
    # tmp_r = X_H.matmul(enc_fore_grad_G)
    #
    # mean1 = tmp_l.mean()
    # mean2 = tmp_r.mean()
    print(X_G)
    print(enc_fore_grad_G)
    tmp1 = (X_G.matmul_c_eql(enc_fore_grad_G)).mean()
    tmp2 = (X_H.matmul_c_eql(enc_fore_grad_G)).mean()

    grad_A =  tmp1.hstack(tmp2)
    learning_rate *= 0.999

  # enc_fore_grad_G.out()
    optim_grad_A = grad_A.scalar_mul(learning_rate)

    pln1 = optim_grad_A.decrypt()
    dco = pln1.decode()
    optim_grad_G = dco.split(10, 1, 0)
    optim_grad_H = dco.split(10, 1, 1)

    w_G = optim_grad_G.sub_local(w_G)
    w_H = optim_grad_H.sub_local(w_H)

    tmp1 = enc_agg_wx_G.matmul(X_Y)
    enc_half_ywx_G = tmp1.scalar_mul(0.5)

    fst = enc_half_ywx_G.scalar_mul(-1)
    sec = enc_agg_wx_square_G.scalar_mul(1/8)
    thd = np.array([[np.log(2)]])

    tmp1 = fst.add(sec)
    tmp2 = tmp1.add_local(thd)

    tmp2.out()



if __name__ == '__main__':
  unittest.main()