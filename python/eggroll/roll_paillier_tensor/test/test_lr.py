import grpc
import time
import unittest
import roll_paillier_tensor as rpt_engine
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RollPaillierTensor as rpt

from eggroll.core.session import ErSession
from eggroll.roll_paillier_tensor.roll_paillier_tensor import RptContext
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.roll_paillier_tensor.test.test_roll_paillier_tensor import TestRollPaillierTensor

class Testlr(unittest.TestCase):
    def test_lr(self):
        session = ErSession(options={"eggroll.deploy.mode": "standalone"})
        context = RptContext(RollPairContext(session))

        mat1 = context.load("ns", "mat_a", "cpu")
        mat2 = context.load("ns", "mat_b", "cpu")

        res = mat1.add(mat2)





if __name__ == '__main__':
    Testlr()
