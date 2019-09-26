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
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle


class TestRollPair(unittest.TestCase):

  def test_map_values(self):
    def append_byte(v):
      return v + b'~1'

    pickled_function = cloudpickle.dumps(append_byte)

    store_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='name')
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

    print(f"result: {result}")

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
