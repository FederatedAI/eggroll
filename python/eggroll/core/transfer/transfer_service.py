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


import queue
from concurrent import futures
from threading import Thread
from time import sleep
from typing import Iterable

import grpc
from grpc._cython import cygrpc

from eggroll.core.conf_keys import TransferConfKeys
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.proto import transfer_pb2_grpc, transfer_pb2
from eggroll.core.utils import _exception_logger
from eggroll.utils import log_utils

log_utils.setDirectory()
LOGGER = log_utils.getLogger()


class TransferService(object):
  data_buffer = dict()
  _DEFAULT_QUEUE_SIZE = 10000

  def start(self, options: {}):
    raise NotImplementedError()

  @staticmethod
  def get_or_create_broker(key: str, maxsize: int = _DEFAULT_QUEUE_SIZE, write_signals=1):
    if not TransferService.has_broker(key):
      print('creating broker: ', key)
      final_size = maxsize if maxsize > 0 else TransferService._DEFAULT_QUEUE_SIZE
      TransferService.data_buffer[key] = \
        FifoBroker(max_capacity=final_size, write_signals=write_signals)

    return TransferService.data_buffer[key]

  @staticmethod
  def get_broker(key: str):
    result = TransferService.data_buffer.get(key, None)
    while not result or key not in TransferService.data_buffer:
      sleep(0.1)
      result = TransferService.data_buffer.get(key, None)

    return result

  @staticmethod
  def remove_broker(key: str):
    result = False
    if key in TransferService.data_buffer:
      del TransferService.data_buffer[key]
      result = True

    return result

  @staticmethod
  def has_broker(key: str):
    return key in TransferService.data_buffer

  @staticmethod
  def transfer_batch_generator_from_broker(broker: FifoBroker, tag):
    i = 0
    while not broker.is_closable():
      try:
        data = broker.get(block=True, timeout=5)
        if data:
          header = transfer_pb2.TransferHeader(id=i, tag=tag)
          batch = transfer_pb2.TransferBatch(header=header, data=data)
          i += 1

          yield batch
      except queue.Empty as e:
        print("transfer client queue empty")
      except Exception as e:
        print(e)

class GrpcTransferServicer(transfer_pb2_grpc.TransferServiceServicer):
  @_exception_logger
  def send(self, request_iterator, context):
    inited = False
    response_header = None

    broker = None
    base_tag = None
    for request in request_iterator:
      if not inited:
        base_tag = request.header.tag
        print('GrpcTransferServicer send broker tag: ', base_tag)
        broker = TransferService.get_broker(base_tag)
        response_header = request.header
        inited = True

      broker.put(request)

    broker.signal_write_finish()
    print('GrpcTransferServicer stream finished. tag: ', base_tag, ', remaining write count: ', broker.get_remaining_write_signal_count())
    return transfer_pb2.TransferBatch(header=response_header)

  @_exception_logger
  def recv(self, request, context):
    base_tag = request.header.tag
    print('GrpcTransferServicer send broker tag: ', base_tag)
    callee_messages_broker: FifoBroker = TransferService.get_broker(base_tag)

    return TransferService.transfer_batch_generator_from_broker(callee_messages_broker, base_tag)


class GrpcTransferService(TransferService):
  def start(self, options: dict = {}):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                         options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                                  (cygrpc.ChannelArgKey.max_receive_message_length, -1)])

    transfer_servicer = GrpcTransferServicer()
    transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer, server)
    port = options.get(TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT, 0)
    port = server.add_insecure_port(f'[::]:{port}')
    LOGGER.info(f'transfer service started at port {port}')
    print(f'transfer service started at port {port}')

    server.start()

    import time
    time.sleep(1000000)


class TransferClient(object):
  def __init__(self):
    self.__grpc_channel_factory = GrpcChannelFactory()
    self.__bin_packet_len = 1 << 20
    self.__chunk_size = 100

  @_exception_logger
  def send(self, broker: FifoBroker, endpoint: ErEndpoint, tag):
    channel = grpc.insecure_channel(
        target=f'{endpoint._host}:{endpoint._port}',
        options=[
          ('grpc.max_send_message_length', -1),
          ('grpc.max_receive_message_length', -1)])

    stub = transfer_pb2_grpc.TransferServiceStub(channel)
    future = stub.send.future(TransferService.transfer_batch_generator_from_broker(broker, tag))

    return future

  @_exception_logger
  def recv(self, endpoint: ErEndpoint, tag):
    def fill_broker(iterable: Iterable, broker):
      iterator = iter(iterable)
      for e in iterator:
        broker.put(e)
      broker.signal_write_finish()

    channel = grpc.insecure_channel(
            target=f'{endpoint._host}:{endpoint._port}',
            options=[
              ('grpc.max_send_message_length', -1),
              ('grpc.max_receive_message_length', -1)])

    stub = transfer_pb2_grpc.TransferServiceStub(channel)
    request = transfer_pb2.TransferBatch(
            header=transfer_pb2.TransferHeader(id=1,
                                               tag=tag))

    response_iter = stub.recv(request)
    broker = FifoBroker()
    t = Thread(target=fill_broker, args=[response_iter, broker])
    t.start()

    # todo:0: exception log?
    return broker