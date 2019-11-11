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
from eggroll.core.proto import transfer_pb2_grpc
from eggroll.core.transfer_model import ErTransferHeader, ErBatch
from eggroll.core.utils import _exception_logger
from queue import Queue


class GrpcTransferServicer(transfer_pb2_grpc.TransferServiceServicer):
  data_buffer = dict()

  _DEFAULT_QUEUE_SIZE = 100
  TRANSFER_END = '__transfer_end'

  @staticmethod
  def get_or_create_broker(key: str, maxsize: int = _DEFAULT_QUEUE_SIZE, write_signals = 1):
    if key not in GrpcTransferServicer.data_buffer:
      final_size = maxsize if maxsize > 0 else GrpcTransferServicer._DEFAULT_QUEUE_SIZE
      GrpcTransferServicer.data_buffer[key] = \
        FifoBroker(max_capacity = final_size, write_signals = write_signals)

    return GrpcTransferServicer.data_buffer[key]

  @_exception_logger
  def send(self, request_iterator, context):
    inited = False
    for request in request_iterator:
      print(f'received')
      if not inited:
        broker = GrpcTransferServicer.get_or_create_broker(request.header.tag)
        response_header = request.header
        inited = True

      broker.put(request.data)
      if request.header.status == GrpcTransferServicer.TRANSFER_END:
        broker.signal_write_finish()

    return response_header


class TransferClient():
  def send(self, data, tag, server_node, status = ''):
    endpoint = server_node._command_endpoint
    channel = grpc.insecure_channel(target=f'{endpoint._host}:{endpoint._port}',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    total_size = 0 if not data else len(data)
    header = ErTransferHeader(id=100,
                              tag=tag,
                              total_size=total_size,
                              status = status)
    batch = ErBatch(header=header, data=data)

    stub = transfer_pb2_grpc.TransferServiceStub(channel)

    batches = [batch.to_proto()]

    #print(f"mw: ready to send to {endpoint}, {iter(batches)}")
    stub.send(iter(batches))

    print("finish send")
