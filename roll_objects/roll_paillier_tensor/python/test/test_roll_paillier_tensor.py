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

import grpc
import time
import unittest
import roll_paillier_tensor as rpt_engine
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_paillier_tensor.roll_paillier_tensor_on_roll_pair import RollPaillierTensorOnRollPair as rpt

class TestRollPaillierTensor(unittest.TestCase):
  def test_scalar_mul(self):
    store_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='mat_a')
    original = rpt(store_locator)
    result = original.scalar_mul(2)

  def test_add(self):
    left_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                  name='mat_a')
    right_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='mat_b')

    left_mat = rpt(left_locator)
    right_mat = rpt(right_locator)
    result = left_mat.add(right_mat)

  def test_gpu_add(self):
      left_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                    name='mat_a')
      right_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                     name='mat_b')

      left_mat = rpt(left_locator)
      right_mat = rpt(right_locator)

      print("[test_gpu_add]: +++___")
      left_mat.gpu_add(right_mat)

  def test_matmul(self):
      left_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                    name='mat_a')
      right_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                     name='mat_b')

      left_mat = rpt(left_locator)
      right_mat = rpt(right_locator)

      print("[test cpu matmul ]: +++___")
      left_mat.mat_mul(right_mat)

  def test_gpu_load(self):
      left_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                    name='mat_a')
      right_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                     name='mat_b')

      left_mat = rpt(left_locator)
      right_mat = rpt(right_locator)

      print("[test_gpu_add]: +++___")
      left_mat.gpu_load(right_mat)

  def test_scalar_mul_raw(self):

    def scalar_mul(v):
      pub_key, private_key = rpt_engine.keygen()

      return rpt_engine.slcmul(rpt_engine.load(v, 1, 1, 1), 2.0, pub_key, private_key)

    pickled_function = cloudpickle.dumps(scalar_mul)

    store_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='mat_a')
    functor = ErFunctor(name="mapValues", body=pickled_function)

    job = ErJob(id="1", name="mapValues",
                inputs=[ErStore(store_locator=store_locator)],
                functors=[functor])

    channel = grpc.insecure_channel(target='localhost:20000',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    roll_pair_stub = command_pb2_grpc.CommandServiceStub(channel)

    request = ErCommandRequest(seq=1,
                               uri='com.webank.eggroll.rollpair.component.RollPair.mapValues',
                               args=[job.to_proto().SerializeToString()])

    print(f"ready to call")
    result = roll_pair_stub.call(request.to_proto())


    time.sleep(1200)

  def test_reduce(self):
    def concat(a, b):
      return a + b

    pickled_function = cloudpickle.dumps(concat)

    store_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='name')
    job = ErJob(id="1", name="reduce",
                inputs=[ErStore(store_locator=store_locator)],
                functors=[ErFunctor(name="reduce", body=pickled_function)])

    channel = grpc.insecure_channel(target='localhost:20000',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    roll_pair_stub = command_pb2_grpc.CommandServiceStub(channel)
    request = ErCommandRequest(seq=1,
                               uri='com.webank.eggroll.rollpair.component.RollPair.reduce',
                               args=[job.to_proto().SerializeToString()])

    result = roll_pair_stub.call(request.to_proto())
    time.sleep(1200)


if __name__ == '__main__':
  unittest.main()
