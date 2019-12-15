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
from grpc._cython import cygrpc
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.proto import transfer_pb2_grpc, transfer_pb2
from eggroll.core.transfer_model import ErTransferHeader, ErTransferBatch
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.utils import _exception_logger
from eggroll.utils import log_utils
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.io.format import BinBatchWriter
from eggroll.core.conf_keys import TransferConfKeys
import sys
from time import sleep
from concurrent import futures
import queue

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


class GrpcTransferServicer(transfer_pb2_grpc.TransferServiceServicer):
  @_exception_logger
  def send(self, request_iterator, context):
    inited = False
    response_header = None

    broker = None
    for request in request_iterator:
      if not inited:
        broker = TransferService.get_broker(request.header.tag)
        response_header = request.header
        inited = True

      broker.put(request)

    broker.signal_write_finish()

    return transfer_pb2.TransferBatch(header=response_header)


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

    def transfer_batch_generator():
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

    stub = transfer_pb2_grpc.TransferServiceStub(channel)
    future = stub.send.future(transfer_batch_generator())

    return future

  @_exception_logger
  def send_pair(self, broker: FifoBroker, tag, processor, status = ''):
    def transfer_batch_generator(packet_len):
      # todo: pull up as format
      buffer = bytearray(packet_len)
      writer = BinBatchWriter({'buffer': buffer})
      cur_offset = writer.get_offset()
      total_written = 0
      LOGGER.info(broker.is_closable())
      while not broker.is_closable():
        k_bytes, v_bytes = broker.get(block=True, timeout=1)
        LOGGER.info("k:{}, v:{}".format(k_bytes, v_bytes))
        if k_bytes is not None:
          try:
            writer.write_bytes(k_bytes, include_size=True)
            writer.write_bytes(v_bytes, include_size=True)
            total_written += 1
            if tag == '1-0':
              LOGGER.info(f'{tag} written {k_bytes}, total_written {total_written}, is closable {broker.is_closable()}')
          except IndexError as e:
            LOGGER.info('caught index error')
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

            LOGGER.info(transfer_batch)
            yield transfer_batch.to_proto()
          except:
            LOGGER.info("Unexpected error:{}".format(sys.exc_info()[0]))
            raise
        else:
          LOGGER.info('k_bytes is None')

      LOGGER.info(f'{tag} is closable, offset: {writer.get_offset()}')
      bin_data = writer.get_batch()
      header = ErTransferHeader(id=100,
                                tag=tag,
                                total_size=writer.get_offset() - 20,
                                status = GrpcTransferServicer.TRANSFER_END)
      transfer_batch = ErTransferBatch(header = header, batch_size = 1, data = bin_data)
      LOGGER.info(transfer_batch)
      yield transfer_batch.to_proto()

    command_endpoint = processor._command_endpoint
    channel = self.__grpc_channel_factory.create_channel(command_endpoint)

    stub = transfer_pb2_grpc.TransferServiceStub(channel)

    LOGGER.info(f'{tag} ready to send')

    future = stub.send.future(iter(transfer_batch_generator(self.__bin_packet_len)))
    LOGGER.info(f'{tag} got future')
    return future

