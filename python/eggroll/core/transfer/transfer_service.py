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
from threading import Thread, Lock
from time import sleep
from typing import Iterable

import grpc
from grpc._cython import cygrpc

from eggroll.core.conf_keys import TransferConfKeys
from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.proto import transfer_pb2_grpc, transfer_pb2
from eggroll.core.utils import _exception_logger
from eggroll.utils.log_utils import get_logger

L = get_logger()


TRANSFER_BROKER_NAME = 'transfer_broker_name'


# TODO:0: thread safe?
class TransferService(object):
    data_buffer = dict()
    _DEFAULT_QUEUE_SIZE = 10000
    mutex = Lock()

    def start(self, options: {}):
        raise NotImplementedError()

    @staticmethod
    def get_or_create_broker(key: str, maxsize: int = _DEFAULT_QUEUE_SIZE, write_signals=1):
        if not TransferService.has_broker(key):
            with TransferService.mutex as m:
                if not TransferService.has_broker(key):
                    L.info(f'creating broker: {key}, write signals: {write_signals}')
                    final_size = maxsize if maxsize > 0 else TransferService._DEFAULT_QUEUE_SIZE
                    TransferService.data_buffer[key] = \
                        FifoBroker(maxsize=final_size, writers=write_signals, name=key)

        return TransferService.data_buffer[key]

    @staticmethod
    def set_broker(key: str, broker):
        TransferService.data_buffer[key] = broker

    @staticmethod
    def get_broker(key: str):
        result = TransferService.data_buffer.get(key, None)
        retry = 0
        while not result or key not in TransferService.data_buffer:
            sleep(min(0.1 * retry, 30))
            L.debug(f"waiting broker tag:{key}, retry:{retry}")
            result = TransferService.data_buffer.get(key, None)
            retry += 1
            if retry > 600:
                raise RuntimeError("cannot get broker:" + key)
        return result

    @staticmethod
    def remove_broker(key: str):
        result = False
        if key in TransferService.data_buffer:
            with TransferService.mutex as m:
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
                data = broker.get(block=True, timeout=0.1)
                if data:
                    header = transfer_pb2.TransferHeader(id=i, tag=tag)
                    batch = transfer_pb2.TransferBatch(header=header, data=data)
                    i += 1

                    yield batch
            except queue.Empty as e:
                # print("transfer client queue empty")
                pass
            except BrokerClosed as e:
                break
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
                L.info(f'GrpcTransferServicer send broker init. tag: {base_tag}')
                broker = TransferService.get_broker(base_tag)
                # response_header = request.header
                # linux error:TypeError: Parameter to MergeFrom() must be instance of same class: expected TransferHeader got TransferHeader. for field TransferBatch.header
                response_header = transfer_pb2.TransferHeader(tag=base_tag, id=request.header.id)
                inited = True

            broker.put(request)
        if inited:
            L.info(f'GrpcTransferServicer stream finished. tag: {base_tag}, remaining write count: {broker,broker.__dict__}, stream not empty')
            result = transfer_pb2.TransferBatch(header=response_header)
        else:
            L.warn(f'broker is None. Getting tag from metadata')
            metadata = dict(context.invocation_metadata())
            base_tag = metadata[TRANSFER_BROKER_NAME]
            broker = TransferService.get_broker(base_tag)
            L.info(f"empty requests for tag: {base_tag}")
            L.info(f'GrpcTransferServicer stream finished. tag: {base_tag}, remaining write count: {broker,broker.__dict__}, stream empty')
            result = transfer_pb2.TransferBatch()

        broker.signal_write_finish()
        return result

    @_exception_logger
    def recv(self, request, context):
        base_tag = request.header.tag
        L.info(f'GrpcTransferServicer recv broker tag: {base_tag}')
        callee_messages_broker = TransferService.get_broker(base_tag)
        import types
        if isinstance(callee_messages_broker, types.GeneratorType):
            i = 0
            for data in callee_messages_broker:
                header = transfer_pb2.TransferHeader(id=i, tag=base_tag)
                batch = transfer_pb2.TransferBatch(header=header, data=data)
                i+=1
                yield batch
        else:
            return TransferService.transfer_batch_generator_from_broker(callee_messages_broker, base_tag)


class GrpcTransferService(TransferService):
    def start(self, options: dict = None):
        if dict is None:
            options = {}
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                             options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                                      (cygrpc.ChannelArgKey.max_receive_message_length, -1)])

        transfer_servicer = GrpcTransferServicer()
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer, server)
        port = options.get(TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT, 0)
        port = server.add_insecure_port(f'[::]:{port}')
        L.info(f'transfer service started at port {port}')
        print(f'transfer service started at port {port}')

        server.start()

        import time
        time.sleep(1000000)


class TransferClient(object):
    def __init__(self):
        self.__grpc_channel_factory = GrpcChannelFactory()
        #self.__bin_packet_len = 32 << 20
        #self.__chunk_size = 100

    @_exception_logger
    def send(self, broker, endpoint: ErEndpoint, tag):
        try:
            channel = self.__grpc_channel_factory.create_channel(endpoint)

            stub = transfer_pb2_grpc.TransferServiceStub(channel)
            import types
            if isinstance(broker, types.GeneratorType):
                requests = (transfer_pb2.TransferBatch(header=transfer_pb2.TransferHeader(id=i, tag=tag), data=d)
                            for i, d in enumerate(broker))
            else:
                requests = TransferService.transfer_batch_generator_from_broker(broker, tag)
            future = stub.send.future(requests, metadata=[(TRANSFER_BROKER_NAME, tag)])

            return future
        except Exception as e:
            L.error(f'Error calling to {endpoint} in TransferClient.send')
            raise e

    @_exception_logger
    def recv(self, endpoint: ErEndpoint, tag, broker):
        try:
            L.debug(f'TransferClient.recv for endpoint: {endpoint}, tag: {tag}')
            @_exception_logger
            def fill_broker(iterable: Iterable, broker):
                try:
                    iterator = iter(iterable)
                    for e in iterator:
                        broker.put(e)
                    broker.signal_write_finish()
                except Exception as e:
                    L.error(f'Fail to fill broker for tag: {tag}, endpoint: {endpoint}')
                    raise e

            channel = self.__grpc_channel_factory.create_channel(endpoint)

            stub = transfer_pb2_grpc.TransferServiceStub(channel)
            request = transfer_pb2.TransferBatch(
                    header=transfer_pb2.TransferHeader(id=1, tag=tag))

            response_iter = stub.recv(
                    request, metadata=[(TRANSFER_BROKER_NAME, tag)])
            if broker is None:
                return response_iter
            else:
                t = Thread(target=fill_broker, args=[response_iter, broker])
                t.start()
            return broker
        except Exception as e:
            L.error(f'Error calling to {endpoint} in TransferClient.recv')
            raise e
