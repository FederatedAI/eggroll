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

store_type = StoreTypes.ROLLPAIR_LEVELDB
max_iter = 1


class TestLR(unittest.TestCase):
  def test_lr(self):
    #base obj
    session = ErSession(session_id="zhanan", options={"eggroll.deploy.mode": "standalone"})
    context = RptContext(RollPairContext(session))
    store = ErStore(store_locator=ErStoreLocator(store_type=store_type, namespace="ns", name="mat_a"))

    #base elem
    X_G_enc = context.load("ns", "mat_a", "cpu")
    X_H_enc = context.load("ns", "mat_b", "cpu")
    X_Y_enc = context.load("ns", "mat_y", "cpu")

    X_G_dco = X_G_enc.decrypt()
    X_H_dco = X_H_enc.decrypt()
    X_Y_dco = X_Y_enc.decrypt()


    # remote
    X_G = X_G_dco.decode()
    X_H = X_H_dco.decode()
    X_Y = X_Y_dco.decode()

    w_G_dco = RptLocalTensor(20, 1, 2, X_G_enc.pub_key, X_G_enc.prv_key)
    w_H_dco = RptLocalTensor(10, 1, 1, X_H_enc.pub_key, X_H_enc.prv_key)

    #local
    w_G = w_G_dco.decode()
    w_H = w_H_dco.decode()

    print("w_G", w_G)
    print("w_H", w_H)

    #fw_G = X_G.matmul_local_numpy(w_G)
    fw_H = X_H.matmul_local_numpy(w_H)

  # iter = 0
    # while iter < max_iter:
    #   fw_H = X_H.matmul_r_eql(w_H)
    #   dec = fw_H.decrypt()
    #   doc = dec.decode()
    #   iter += 1



if __name__ == '__main__':
  unittest.main()