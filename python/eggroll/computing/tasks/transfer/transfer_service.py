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
import io
import logging
import os
import queue
import zipfile
from threading import Thread, Lock, Event
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZIP_STORED

import grpc
from grpc._cython import cygrpc

from eggroll.config import Config, ConfigKey, ConfigUtils
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.proto import (
    transfer_pb2_grpc,
    transfer_pb2,
    deepspeed_download_pb2_grpc,
    deepspeed_download_pb2,
)
from eggroll.core.proto.containers_pb2 import ContentType, ContainerContent
from eggroll.trace import exception_catch

L = logging.getLogger(__name__)

TRANSFER_BROKER_NAME = "transfer_broker_name"


# TODO:0: thread safe?
class TransferService(object):
    data_buffer = dict()
    event_buffer = dict()
    _DEFAULT_QUEUE_SIZE = 10000
    mutex = Lock()

    def start(self, options: {}):
        raise NotImplementedError()

    @staticmethod
    def get_or_create_broker(
        config: Config, key: str, maxsize: int = _DEFAULT_QUEUE_SIZE, write_signals=1
    ):
        with TransferService.mutex:
            if not TransferService.has_broker(key):
                L.debug(f"creating broker={key}, write signals={write_signals}")
                final_size = (
                    maxsize if maxsize > 0 else TransferService._DEFAULT_QUEUE_SIZE
                )
                TransferService.data_buffer[key] = FifoBroker(
                    config=config, maxsize=final_size, writers=write_signals, name=key
                )
            if key not in TransferService.event_buffer:
                TransferService.event_buffer[key] = Event()
            TransferService.event_buffer[key].set()

            return TransferService.data_buffer[key]

    @staticmethod
    def set_broker(key: str, broker):
        with TransferService.mutex as m:
            TransferService.data_buffer[key] = broker
            if key not in TransferService.event_buffer:
                TransferService.event_buffer[key] = Event()
            TransferService.event_buffer[key].set()

    @staticmethod
    def get_broker(key: str):
        result = None
        retry = 0
        while True:
            report_interval = 60
            L.debug(
                f"waiting broker tag={key}, retry={retry}, report_interval={report_interval}"
            )
            event = None
            with TransferService.mutex:
                if key not in TransferService.event_buffer:
                    TransferService.event_buffer[key] = Event()

            TransferService.event_buffer[key].wait(report_interval)
            with TransferService.mutex:
                if TransferService.event_buffer[key].is_set():
                    L.debug(f"event is set. tag={key}")
                    result = TransferService.data_buffer.get(key)
                else:
                    L.debug(f"event is not set. tag={key}")
                if result is not None:
                    break
                else:
                    L.debug(f"result is None. tag={key}")
            retry += 1
            if retry > 1:
                raise RuntimeError(
                    f"cannot get broker={key}, result={result}, data_buffer={TransferService.data_buffer}, event_buffer={TransferService.event_buffer}"
                )
        return result

    @staticmethod
    def remove_broker(key: str):
        L.debug(f"trying to remove broker tag={key}")
        result = False

        event = None
        with TransferService.mutex as m:
            if key in TransferService.data_buffer:
                L.debug(f"actual removing broker tag={key}")
                data = TransferService.data_buffer[key]
                del TransferService.data_buffer[key]
                event = TransferService.event_buffer[key]
                del TransferService.event_buffer[key]
                result = True

            if event is not None and not event.is_set():
                L.debug(f"removing event but it is not set: key={key}")

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
                # todo:1: remove print here
                print(e)


class GrpcDsDownloadServicer(deepspeed_download_pb2_grpc.DsDownloadServiceServicer):
    def __init__(self, config: Config):
        self.config = config

    def get_container_workspace(self, session_id, rank):
        data_dir = self.config.eggroll.resourcemanager.nodemanager.containers.data.dir
        return os.path.join(data_dir, session_id, rank)

    def get_container_models_dir(self, session_id, rank):
        return os.path.join(self.get_container_workspace(session_id, rank), "models")

    def get_container_logs_dir(self, session_id, rank):
        return os.path.join(self.get_container_workspace(session_id, rank), "logs")

    def get_container_result_dir(self, session_id, rank):
        return os.path.join(self.get_container_workspace(session_id, rank), "result")

    def get_container_path(self, content_type, session_id, rank):
        # case ContentType.ALL => getContainerWorkspace(containerId)
        # case ContentType.MODELS => getContainerModelsDir(containerId)
        # case ContentType.LOGS => getContainerLogsDir(containerId)
        #

        if content_type == ContentType.ALL:
            return self.get_container_workspace(session_id, rank)
        elif content_type == ContentType.MODELS:
            return self.get_container_models_dir(session_id, rank)
        elif content_type == ContentType.LOGS:
            return self.get_container_logs_dir(session_id, rank)
        elif content_type == ContentType.RESULT:
            return self.get_container_result_dir(session_id, rank)
        else:
            raise RuntimeError(f"download content type {content_type} is not support ")

    @exception_catch
    def download(self, request: deepspeed_download_pb2.DsDownloadRequest, context):
        L.info(f"receive download request  {request}")
        result = []
        try:
            for rank in request.ranks:
                L.info(f"prepare to download container_id {rank}")
                path = self.get_container_path(
                    request.content_type, request.session_id, str(rank)
                )
                L.info(f"prepare to download path {path}")
                content = zip2bytes(startdir=path)
                compress_content = ContainerContent(rank=rank, content=content)
                # message ContainerContent
                # {
                #     int64 container_id = 1;
                #     bytes content = 2;
                #     string compress_method = 3;
                # }
                result.append(compress_content)
        except Exception as e:
            L.exception(f"download error request  {request}")
            raise e
        return deepspeed_download_pb2.DsDownloadResponse(
            session_id=request.session_id, container_content=result
        )

    @exception_catch
    def download_by_split(self, request, context):
        L.info(f"receive download_by_split request  {request}")
        result = []
        try:
            for rank in request.ranks:
                L.info(f"prepare to download container_id {rank}")
                path = self.get_container_path(
                    request.content_type, request.session_id, str(rank)
                )
                L.info(f"prepare to download path {path}")
                content = zip2bytes(startdir=path)
                result.append((rank, content))
        except Exception as e:
            L.exception(f"download error request  {request}")
            raise e

        return chunker2(result, 1024 * 1024 * 1024)


class GrpcTransferServicer(transfer_pb2_grpc.TransferServiceServicer):
    @exception_catch
    def send(self, request_iterator, context):
        inited = False
        response_header = None

        broker = None
        base_tag = None
        try:
            for request in request_iterator:
                if not inited:
                    base_tag = request.header.tag
                    L.debug(f"GrpcTransferServicer send broker init. tag={base_tag}")
                    broker = TransferService.get_broker(base_tag)
                    # response_header = request.header
                    # linux error:TypeError: Parameter to MergeFrom() must be instance of same class: expected TransferHeader got TransferHeader. for field TransferBatch.header
                    response_header = transfer_pb2.TransferHeader(
                        tag=base_tag, id=request.header.id, ext=request.header.ext
                    )
                    inited = True

                broker.put(request)
            if inited:
                L.debug(
                    f"GrpcTransferServicer stream finished. tag={base_tag}, remaining write count={broker, broker.__dict__}, stream not empty"
                )
                result = transfer_pb2.TransferBatch(header=response_header)
            else:
                raise ValueError(
                    "error in GrpcTransferServicer.send: empty request_iterator"
                )

            return result
        except Exception as e:
            TransferService.remove_broker(base_tag)
            raise ValueError(f"error in processing {base_tag}", e)
        finally:
            if broker is not None:
                broker.signal_write_finish()

    @exception_catch
    def recv(self, request, context):
        base_tag = request.header.tag
        L.debug(f"GrpcTransferServicer recv broker tag={base_tag}")
        callee_messages_broker = TransferService.get_broker(base_tag)
        import types

        if isinstance(callee_messages_broker, types.GeneratorType):
            i = 0
            for data in callee_messages_broker:
                header = transfer_pb2.TransferHeader(id=i, tag=base_tag)
                batch = transfer_pb2.TransferBatch(header=header, data=data)
                i += 1
                yield batch
        else:
            return TransferService.transfer_batch_generator_from_broker(
                callee_messages_broker, base_tag
            )


class GrpcTransferService(TransferService):
    def start(self, config: Config, options: dict = None):
        if dict is None:
            options = {}
        _executor_pool_type = ConfigUtils.get_option(
            config, option=options, key=ConfigKey.eggroll.core.default.executor.pool
        )
        server = grpc.server(
            create_executor_pool(
                canonical_name=_executor_pool_type,
                max_workers=1,
                thread_name_prefix="roll_pair_transfer_service",
            ),
            options=[
                (cygrpc.ChannelArgKey.max_send_message_length, -1),
                (cygrpc.ChannelArgKey.max_receive_message_length, -1),
            ],
        )

        transfer_servicer = GrpcTransferServicer()
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(
            transfer_servicer, server
        )
        port = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.transfer.service.port
        )
        port = server.add_insecure_port(f"[::]:{port}")
        L.info(f"transfer service started at port={port}")
        print(f"transfer service started at port={port}")

        server.start()

        import time

        time.sleep(1000000)


class TransferClient(object):
    def __init__(self):
        self.__grpc_channel_factory = GrpcChannelFactory()
        # self.__bin_packet_len = 32 << 20
        # self.__chunk_size = 100

    @exception_catch
    def send(self, config: Config, broker, endpoint: ErEndpoint, tag):
        try:
            L.debug(f"TransferClient.send for endpoint={endpoint}, tag={tag}")
            channel = self.__grpc_channel_factory.create_channel(
                config=config, endpoint=endpoint
            )

            stub = transfer_pb2_grpc.TransferServiceStub(channel)
            import types

            if isinstance(broker, types.GeneratorType):
                requests = (
                    transfer_pb2.TransferBatch(
                        header=transfer_pb2.TransferHeader(id=i, tag=tag), data=d
                    )
                    for i, d in enumerate(broker)
                )
            else:
                requests = TransferService.transfer_batch_generator_from_broker(
                    broker, tag
                )
            future = stub.send.future(requests, metadata=[(TRANSFER_BROKER_NAME, tag)])

            return future
        except Exception as e:
            L.exception(f"Error calling to {endpoint} in TransferClient.send")
            raise e

    @exception_catch
    def recv(self, config: Config, endpoint: ErEndpoint, tag, broker):
        exception = None
        cur_retry = 0
        for cur_retry in range(3):
            try:
                L.debug(f"TransferClient.recv for endpoint={endpoint}, tag={tag}")

                @exception_catch
                def fill_broker(iterable: Iterable, broker):
                    try:
                        iterator = iter(iterable)
                        for e in iterator:
                            broker.put(e)
                        broker.signal_write_finish()
                    except Exception as e:
                        L.exception(
                            f"Fail to fill broker for tag: {tag}, endpoint: {endpoint}"
                        )
                        raise e

                channel = self.__grpc_channel_factory.create_channel(
                    config=config, endpoint=endpoint
                )

                stub = transfer_pb2_grpc.TransferServiceStub(channel)
                request = transfer_pb2.TransferBatch(
                    header=transfer_pb2.TransferHeader(id=1, tag=tag)
                )

                response_iter = stub.recv(
                    request, metadata=[(TRANSFER_BROKER_NAME, tag)]
                )

                if broker is None:
                    return response_iter
                else:
                    t = Thread(target=fill_broker, args=[response_iter, broker])
                    t.start()
                return broker
            except Exception as e:
                L.warning(
                    f"Error calling to {endpoint} in TransferClient.recv, cur_retry={cur_retry}",
                    exc_info=e,
                )
                exception = e
                cur_retry += 1

        if exception is not None:
            L.exception(
                f"fail to {endpoint} in TransferClient.recv, cur_retry={cur_retry}",
                exc_info=e,
            )
            raise exception


def zip2bytes(startdir, compression=ZIP_DEFLATED, compresslevel=1, **kwargs) -> bytes:
    L.info(f"download start dir {startdir}")
    if compression == ZIP_STORED:
        compresslevel = None
        # kwargs["compression"] = compression
        # kwargs["compresslevel"] = compresslevel
    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer, "w", compression=compression, compresslevel=compresslevel
    ) as z:
        for dirpath, dirnames, filenames in os.walk(startdir):
            for filename in filenames:
                subpath = os.path.join(dirpath, filename)

                with open(subpath, "rb") as subfile:
                    z.writestr(filename, subfile.read())
    buffer.seek(0)
    return buffer.read()


def chunker(iterable, size):
    for i in range(0, len(iterable), size):
        yield deepspeed_download_pb2.DsDownloadSplitResponse(
            data=iterable[i : i + size]
        )


def chunker2(iterable, size):
    for j in iterable:
        for i in range(0, len(j[1]), size):
            yield deepspeed_download_pb2.DsDownloadSplitResponse(
                data=j[1][i : i + size], rank=j[0]
            )
