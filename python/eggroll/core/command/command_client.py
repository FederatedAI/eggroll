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


import logging
import threading
import time
import typing

from eggroll import trace
from eggroll.config import Config
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import CommandURI, ErCommandRequest, ErCommandResponse
from eggroll.core.meta_model import (
    ErEndpoint,
    ErServerNode,
    ErServerCluster,
    ErProcessor,
    ErStoreList,
    ErStore,
    ErSessionMeta,
    RpcMessage,
    to_proto_string,
    map_and_listify,
)
from eggroll.core.proto import command_pb2_grpc
from .command_uri import (
    MetadataCommands,
    NodeManagerCommands,
    SessionCommands,
)

L = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class CommandCallError(Exception):
    def __init__(
        self, command_uri: CommandURI, endpoint: ErEndpoint, *args: object
    ) -> None:
        super().__init__(
            f"Failed to call command: {command_uri} to endpoint: {endpoint}, caused by: ",
            *args,
        )


T = typing.TypeVar("T")


class CommandClient(object):
    _executor_pool = None
    _executor_pool_lock = threading.Lock()

    def __init__(self, config: Config):
        self._config = config
        self._channel_factory = GrpcChannelFactory()
        self._maybe_create_executor_pool(config)

    @classmethod
    def _maybe_create_executor_pool(cls, config: Config):
        if CommandClient._executor_pool is None:
            with CommandClient._executor_pool_lock:
                if CommandClient._executor_pool is None:
                    _executor_pool_type = config.eggroll.core.default.executor.pool
                    _max_workers = (
                        config.eggroll.core.client.command.executor.pool.max.size
                    )
                    CommandClient._executor_pool = create_executor_pool(
                        canonical_name=_executor_pool_type,
                        max_workers=_max_workers,
                        thread_name_prefix="command_client",
                    )

    def simple_sync_send(
        self,
        input: RpcMessage,
        output_type: typing.Type[T],
        endpoint: ErEndpoint,
        command_uri: CommandURI,
    ) -> T:
        results = self.sync_send(
            inputs=[input],
            output_types=[] if output_type is None else [output_type],
            endpoint=endpoint,
            command_uri=command_uri,
        )

        if len(results):
            return results[0]
        else:
            return None

    def sync_send(
        self,
        inputs: list,
        output_types: list,
        endpoint: ErEndpoint,
        command_uri: CommandURI,
    ):
        with tracer.start_span("sync_send") as span:
            request = None
            try:
                request = ErCommandRequest(
                    uri=command_uri.uri,
                    args=map_and_listify(to_proto_string, inputs),
                )
                span.set_attribute("endpoint", f"{endpoint}")
                span.set_attribute("command_uri", f"{command_uri}")
                span.set_attribute("request", f"{request}")
                _channel = self._channel_factory.create_channel(self._config, endpoint)
                _command_stub = command_pb2_grpc.CommandServiceStub(_channel)
                response = _command_stub.call(request.to_proto())
                er_response = ErCommandResponse.from_proto(response)
                span.set_attribute("response", f"{er_response}")
                byte_results = er_response._results

                if len(byte_results):
                    zipped = zip(output_types, byte_results)
                    return list(
                        map(
                            lambda t: t[0].from_proto_string(t[1])
                            if t[1] is not None
                            else None,
                            zipped,
                        )
                    )
                else:
                    return []
            except Exception as e:
                L.exception(
                    f"Error calling to {endpoint}, command_uri: {command_uri}, req:{request}"
                )
                raise CommandCallError(command_uri, endpoint, e) from e

    def async_call(
        self, args, output_types: list, command_uri: CommandURI, callback=None
    ):
        futures = list()
        for inputs, endpoint in args:
            f = self._executor_pool.submit(
                self.sync_send,
                inputs=inputs,
                output_types=output_types,
                endpoint=endpoint,
                command_uri=command_uri,
            )
            if callback:
                f.add_done_callback(callback)
            futures.append(f)

        return futures


class ClusterManagerClient(object):
    def __init__(self, config: Config, host=None, port=None):
        if host is None:
            host = config.eggroll.resourcemanager.clustermanager.host
        if port is None:
            port = config.eggroll.resourcemanager.clustermanager.port
        self.__endpoint = ErEndpoint(host, int(port))
        self.__command_client = CommandClient(config=config)

    def get_server_node(self, input: ErServerNode):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErServerNode,
            command_uri=MetadataCommands.GET_SERVER_NODE,
        )

    def get_server_nodes(self, input: ErServerNode):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErServerCluster,
            command_uri=MetadataCommands.GET_SERVER_NODES,
        )

    def get_or_create_server_node(self, input: ErServerNode):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErServerNode,
            command_uri=MetadataCommands.GET_OR_CREATE_SERVER_NODE,
        )

    def create_or_update_server_node(self, input: ErServerNode):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErServerNode,
            command_uri=MetadataCommands.CREATE_OR_UPDATE_SERVER_NODE,
        )

    def get_store(self, input: ErStore) -> ErStore:
        return self.__do_sync_request_internal(
            input=input, output_type=ErStore, command_uri=MetadataCommands.GET_STORE
        )

    def get_or_create_store(self, input: ErStore) -> ErStore:
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErStore,
            command_uri=MetadataCommands.GET_OR_CREATE_STORE,
        )

    def delete_store(self, input: ErStore):
        return self.__do_sync_request_internal(
            input=input, output_type=ErStore, command_uri=MetadataCommands.DELETE_STORE
        )

    def get_store_from_namespace(self, input):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErStoreList,
            command_uri=MetadataCommands.GET_STORE_FROM_NAMESPACE,
        )

    def get_or_create_session(self, input: ErSessionMeta):
        return self.__check_processors(
            self.__do_sync_request_internal(
                input=input,
                output_type=ErSessionMeta,
                command_uri=SessionCommands.GET_OR_CREATE_SESSION,
            )
        )

    def register_session(self, session_meta: ErSessionMeta):
        return self.__check_processors(
            self.__command_client.sync_send(
                inputs=[session_meta],
                output_types=[ErSessionMeta],
                endpoint=self.__endpoint,
                command_uri=SessionCommands.REGISTER_SESSION,
            )[0]
        )

    def get_session_server_nodes(self, input: ErSessionMeta):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErServerCluster,
            command_uri=SessionCommands.GET_SESSION_SERVER_NODES,
        )

    def heartbeat(self, input: ErProcessor):
        return self.__do_sync_request_internal(
            input=input, output_type=ErProcessor, command_uri=SessionCommands.HEARTBEAT
        )

    def stop_session(self, input: ErSessionMeta):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErSessionMeta,
            command_uri=SessionCommands.STOP_SESSION,
        )

    def kill_session(self, input: ErSessionMeta):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErSessionMeta,
            command_uri=SessionCommands.KILL_SESSION,
        )

    def kill_all_sessions(self):
        return self.__do_sync_request_internal(
            input=ErSessionMeta(),
            output_type=ErSessionMeta,
            command_uri=SessionCommands.KILL_ALL_SESSIONS,
        )

    def __do_sync_request_internal(self, input, output_type, command_uri):
        return self.__command_client.simple_sync_send(
            input=input,
            output_type=output_type,
            endpoint=self.__endpoint,
            command_uri=command_uri,
        )

    def __check_processors(self, session_meta: ErSessionMeta):
        if not session_meta.is_processors_valid():
            raise ValueError(f"processor in session meta is not valid: {session_meta}")
        return session_meta


class NodeManagerClient(object):
    def __init__(self, config: Config, host, port):
        self.__endpoint = ErEndpoint(
            host=host,
            port=port,
        )
        self.__command_client = CommandClient(config=config)

    """
    def get_or_create_servicer(self, session_meta: ErSessionMeta):
        result = self.__do_sync_request_internal(
                input=session_meta,
                output_type=ErProcessorBatch,
                command_uri=NodeManagerCommands.GET_OR_CREATE_SERVICER,
                serdes_type=self.__serdes_type)
        return result

    def get_or_create_processor_batch(self, session_meta: ErSessionMeta):
        return self.__do_sync_request_internal(
                input=session_meta,
                output_type=ErProcessorBatch,
                command_uri=NodeManagerCommands.GET_OR_CREATE_PROCESSOR_BATCH,
                serdes_type=self.__serdes_type)
    """

    def heartbeat(self, processor: ErProcessor):
        return self.__do_sync_request_internal(
            input=processor,
            output_type=ErProcessor,
            command_uri=NodeManagerCommands.HEARTBEAT,
        )

    def __do_sync_request_internal(self, input, output_type, command_uri):
        return self.__command_client.simple_sync_send(
            input=input,
            output_type=output_type,
            endpoint=self.__endpoint,
            command_uri=command_uri,
        )
