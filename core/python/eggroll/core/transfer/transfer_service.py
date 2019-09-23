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
from eggroll.core.proto import transfer_pb2_grpc
from eggroll.core.transfer_models import ErTransferHeader, ErBatch
from eggroll.core.utils import _exception_logger
from queue import Queue


class TransferServicer(transfer_pb2_grpc.TransferServiceServicer):
  data_buffer = dict()

  _DEFAULT_QUEUE_SIZE = 100

  @staticmethod
  def get_or_create_queue(key: str, max_size: int = _DEFAULT_QUEUE_SIZE):
    if key not in TransferServicer.data_buffer:
      final_size = max_size if max_size > 0 else TransferServicer._DEFAULT_QUEUE_SIZE
      TransferServicer.data_buffer[key] = Queue(maxsize=final_size)

    return TransferServicer.data_buffer[key]

  @_exception_logger
  def send(self, request_iterator, context):
    inited = False
    for request in request_iterator:
      print(f'received')
      if not inited:
        queue = TransferServicer.get_or_create_queue(request.header.tag)
        response_header = request.header
        inited = True

      queue.put(request.data)

    return response_header


class TransferClient():
  def send(self, data, tag, server_node):
    endpoint = server_node._endpoint
    channel = grpc.insecure_channel(target=f'{endpoint._host}:{endpoint._port}',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    header = ErTransferHeader(id=100, tag=tag, total_size=len(data))
    batch = ErBatch(header=header, data=data)

    stub = transfer_pb2_grpc.TransferServiceStub(channel)

    batches = [batch.to_proto()]

    print(f"mw: ready to send to {endpoint}, {iter(batches)}")
    stub.send(iter(batches))

    print("finish send")
