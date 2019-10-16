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

from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse
from eggroll.core.meta import ErStoreLocator, ErJob, ErStore, ErFunctor, ErTask
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
import grpc
import time

class RollPair(object):
  __uri_prefix = 'RollPair.'
  MAP_VALUES = 'mapValues'
  REDUCE = 'reduce'
  JOIN = 'join'
  CLOUD_PICKLE = 'cloudpickle'

  def __init__(self, store_desc = None):
    self._channel = grpc.insecure_channel(
        target='localhost:20000',
        options=[('grpc.max_send_message_length', -1),
                 ('grpc.max_receive_message_length', -1)])
    self._roll_pair_stub = command_pb2_grpc.CommandServiceStub(self._channel)
    self._seq = 1
    self._session_id = '1'

    self.land(store_desc)

  def __repr__(self):
    return f'python RollPair(_store={self._store})'

  def land(self, store_desc):
    if not store_desc:
      self._store = None

    if isinstance(store_desc, ErStore):
      self._store = store_desc
    elif isinstance(store_desc, ErStoreLocator):
      self._store = ErStore(store_locator=store_desc)
    else:
      raise ValueError('illegal parameter type for store_desc')

  def __get_seq(self):
    self._seq = self._seq + 1
    return self._seq

  # computing api
  def map_values(self, func):
    functor = ErFunctor(name=RollPair.MAP_VALUES, serdes=RollPair.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    job = ErJob(id=self._session_id, name=RollPair.MAP_VALUES,
                inputs=[self._store],
                functors=[functor])
    request = ErCommandRequest(seq=self.__get_seq(), uri=f'{RollPair.__uri_prefix}{RollPair.MAP_VALUES}', args=[job.to_proto().SerializeToString()])
    response = self._roll_pair_stub.call(request.to_proto())

    des_response = ErCommandResponse.from_proto(response)

    des_store = ErStore.from_proto_string(des_response._data)

    return RollPair(des_store)

  def reduce(self, func):
    functor = ErFunctor(name=RollPair.REDUCE, serdes=RollPair.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    job = ErJob(id=self._session_id, name=RollPair.REDUCE,
                inputs=[self._store],
                functors=[functor])

    request = ErCommandRequest(seq=self.__get_seq(),
                               uri=f'{RollPair.__uri_prefix}{RollPair.REDUCE}',
                               args=[job.to_proto().SerializeToString()])

    response = self._roll_pair_stub.call(request.to_proto())

    des_response = ErCommandResponse.from_proto(response)

    des_store = ErStore.from_proto_string(des_response._data)

    return RollPair(des_store)

  def join(self, other, func):
    functor = ErFunctor(name=RollPair.JOIN, serdes=RollPair.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    job = ErJob(id=self._session_id, name=RollPair.JOIN,
                inputs=[self._store, other._store],
                functors=[functor])

    request = ErCommandRequest(seq=self.__get_seq(),
                              uri=f'{RollPair.__uri_prefix}{RollPair.JOIN}',
                              args=[job.to_proto().SerializeToString()])

    response = self._roll_pair_stub.call(request.to_proto())

    des_response = ErCommandResponse.from_proto(response)

    des_store = ErStore.from_proto_string(des_response._data)

    return RollPair(des_store)
