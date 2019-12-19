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
from eggroll.roll_paillier_tensor.roll_paillier_tensor import NumpyTensor
from eggroll.roll_pair.roll_pair import RollPairContext, RollPair
from eggroll.roll_paillier_tensor.test.test_roll_paillier_tensor import TestRollPaillierTensor

import numpy as np
import pandas as pd

store_type = StoreTypes.ROLLPAIR_LEVELDB
max_iter = 1


class TestLR(unittest.TestCase):
  def test_lr(self):
    #base obj
    session = ErSession(session_id="zhanan", options={"eggroll.deploy.mode": "standalone"})
    rp_context = RollPairContext(session)
    context = RptContext(rp_context)
    store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

    # #base elem
    # X_H_enc = context.load("ns", "mat_a", "cpu")
    # X_G_enc = context.load("ns", "mat_b", "cpu")
    # X_Y_enc = context.load("ns", "mat_y", "cpu")
    #
    # X_H = X_H_enc.decrypt()
    # X_G = X_G_enc.decrypt()
    # X_Y = X_Y_enc.decrypt()


    data_G = pd.read_csv("/data/czn/data/breast_b23_25X12_mini.csv").values
    data_H = pd.read_csv("/data/czn/data/breast_a23_25X21_mini.csv").values

    rp_x_G = rp_context.load('ns', 'rp_x_G')
    rp_x_H = rp_context.load('ns', 'rp_x_H')
    rp_x_Y = rp_context.load('ns', 'rp_x_Y')

    #X_G X_H
    rp_x_G.put('1', np.array([data_G[0][2:]]))
    rp_x_H.put('1', np.array([data_H[0][1:]]))
    rp_x_Y.put('1', np.array([[data_G[0][1]]]))
    X_G = RollPaillierTensor(rp_x_G)
    X_H = RollPaillierTensor(rp_x_H)
    X_Y = RollPaillierTensor(rp_x_Y)

    #squre
    fw_G2 = (np.array([data_G[0][2:]])).dot(np.ones((10, 1))) ** 2
    fw_H2 = (np.array([data_H[0][1:]])).dot(np.ones((20, 1))) ** 2
    rp_x_H2 = rp_context.load('ns', 'rp_x_H2')
    rp_x_G2 = rp_context.load('ns', 'rp_x_G2')
    rp_x_H2.put('1', fw_H2)
    rp_x_G2.put('1', fw_G2)
    fw_H2 = RollPaillierTensor(rp_x_H2)
    fw_G2 = RollPaillierTensor(rp_x_G2)

    w_H = NumpyTensor(np.ones((20, 1)))
    w_G = NumpyTensor(np.ones((10, 1)))

    learning_rate=0.15
    itr = 0
    pre_loss_A = None
    while itr < max_iter:
      fw_H = X_H @ w_H
      enc_fw_H = fw_H.encrypt()
      enc_fw_square_H = fw_H2.encrypt()

      fw_G = X_G @ w_G
      enc_fw_G = fw_G.encrypt()
      enc_fw_square_G = fw_G2.encrypt()

      enc_agg_wx_G = enc_fw_G + enc_fw_H
      enc_agg_wx_square_G = enc_fw_square_G + enc_fw_square_H + fw_G * enc_fw_H * 2

      enc_fore_grad_G = enc_agg_wx_G * 0.25 - X_Y * 0.5

      enc_grad_G = (X_G * enc_fore_grad_G).mean()
      enc_grad_H = (X_H * enc_fore_grad_G).mean()

      grad_A = enc_grad_G.hstack(enc_grad_H)

      learning_rate *= 0.999
      optim_grad_A = grad_A * learning_rate

      optim_grad_G, optim_grad_H = optim_grad_A.decrypt().split(10, 1)

      w_G = w_G.T() - optim_grad_G
      w_H = w_H.T() - optim_grad_H

      enc_half_ywx_G = enc_agg_wx_G * 0.5 * X_Y
      #todo diversion
      enc_loss_G = ((enc_half_ywx_G * -1) + enc_agg_wx_square_G / 8 + NumpyTensor(np.log(2))).mean()
      loss_A = enc_loss_G.decrypt().store
	  #########################
      tmp = 99999 if pre_loss_A is None else loss_A - pre_loss_A
      if pre_loss_A is not None and abs(loss_A - pre_loss_A) < 1e-4:
        break
      pre_loss_A = loss_A
      itr += 1
	  #########################

if __name__ == '__main__':
  unittest.main()