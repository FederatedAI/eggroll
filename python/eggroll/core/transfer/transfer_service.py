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
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.proto import transfer_pb2_grpc, transfer_pb2
from eggroll.core.transfer_model import ErTransferHeader, ErTransferBatch
from eggroll.core.utils import _exception_logger
from eggroll.core.grpc.factory import GrpcChannelFactory
from queue import Queue
from eggroll.core.io.format import BinBatchWriter
import sys
from time import sleep


class GrpcTransferServicer(transfer_pb2_grpc.TransferServiceServicer):
  data_buffer = dict()

  _DEFAULT_QUEUE_SIZE = 100
  TRANSFER_END = '__transfer_end'

  # todo: move out
  @staticmethod
  def get_or_create_broker(key: str, maxsize: int = _DEFAULT_QUEUE_SIZE, write_signals = 1):
    if key not in GrpcTransferServicer.data_buffer:
      final_size = maxsize if maxsize > 0 else GrpcTransferServicer._DEFAULT_QUEUE_SIZE
      GrpcTransferServicer.data_buffer[key] = \
        FifoBroker(max_capacity = final_size, write_signals = write_signals)

    return GrpcTransferServicer.data_buffer[key]

  @staticmethod
  def get_broker(key: str):
    result = GrpcTransferServicer.data_buffer.get(key, None)
    while not result or key not in GrpcTransferServicer.data_buffer:
      sleep(0.1)
      result = GrpcTransferServicer.data_buffer.get(key, None)

    return result

  @staticmethod
  def remove_broker(key: str):
    result = False
    if key in GrpcTransferServicer.data_buffer:
      del GrpcTransferServicer.data_buffer[key]
      result = True

    return result

  @_exception_logger
  def send(self, request_iterator, context):
    inited = False
    response_header = None
    for request in request_iterator:
      if not inited:
        broker = GrpcTransferServicer.get_broker(request.header.tag)
        response_header = request.header
        inited = True

      broker.put(request)
      if request.header.status == GrpcTransferServicer.TRANSFER_END:
        broker.signal_write_finish()

    return transfer_pb2.TransferBatch(header=response_header)

# todo: move to core/client
class TransferClient(object):
  def __init__(self):
    self.__grpc_channel_factory = GrpcChannelFactory()
    self.__bin_packet_len = 1 << 20
    self.__chunk_size = 100

  def send_single(self, data, tag, processor, status = ''):
    endpoint = processor._command_endpoint
    channel = grpc.insecure_channel(target=f'{endpoint._host}:{endpoint._port}',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    total_size = 0 if not data else len(data)
    header = ErTransferHeader(id=100,
                              tag=tag,
                              total_size=total_size,
                              status = status)
    batch = ErTransferBatch(header=header, data=data)

    stub = transfer_pb2_grpc.TransferServiceStub(channel)

    batches = [batch.to_proto()]

    #print(f"mw: ready to send to {endpoint}, {iter(batches)}")
    stub.send(iter(batches))

    print("finish send")

  def send_pair(self, broker: FifoBroker, tag, processor, status = ''):
    def transfer_batch_generator(packet_len):
      # todo: pull up as format
      buffer = bytearray(packet_len)
      writer = BinBatchWriter({'buffer': buffer})
      cur_offset = writer.get_offset
      total_written = 0
      while not broker.is_closable():
        k_bytes, v_bytes = broker.get(block=True, timeout=1)
        if k_bytes is not None:
          try:
            writer.write_bytes(k_bytes, include_size=True)
            writer.write_bytes(v_bytes, include_size=True)
            total_written += 1
            if tag == '1-0':
              print(f'{tag} written {k_bytes}, total_written {total_written}, is closable {broker.is_closable()}')
          except IndexError as e:
            print('caught index error')
            bin_data = writer.get_batch(end=cur_offset)
            header = ErTransferHeader(id=100,
                                      tag=tag,
                                      total_size=writer.get_offset - 20,
                                      status = '')
            transfer_batch = ErTransferBatch(header = header, batch_size = 1, data = bin_data)

            buffer = bytearray(self.__bin_packet_len)
            writer = BinBatchWriter({'buffer': buffer})
            writer.write_bytes(k_bytes, include_size=True)
            writer.write_bytes(v_bytes, include_size=True)

            print(transfer_batch)
            yield transfer_batch.to_proto()
          except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        else:
          print('k_bytes is None')

      print(f'{tag} is closable, offset: {writer.get_offset()}')
      bin_data = writer.get_batch()
      header = ErTransferHeader(id=100,
                                tag=tag,
                                total_size=writer.get_offset() - 20,
                                status = GrpcTransferServicer.TRANSFER_END)
      transfer_batch = ErTransferBatch(header = header, batch_size = 1, data = bin_data)
      print(transfer_batch)
      yield transfer_batch.to_proto()

    command_endpoint = processor._command_endpoint
    channel = self.__grpc_channel_factory.create_channel(command_endpoint)

    stub = transfer_pb2_grpc.TransferServiceStub(channel)

    print (f'{tag} ready to send')

    future = stub.send.future(iter(transfer_batch_generator(self.__bin_packet_len)))
    print(f'{tag} got future')
    return future

