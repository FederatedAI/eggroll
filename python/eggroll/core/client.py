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


import time
import threading


from eggroll.core.base_model import RpcMessage
from eggroll.core.command.command_model import CommandURI
from eggroll.core.command.command_model import ErCommandRequest, \
    ErCommandResponse
from eggroll.core.command.commands import MetadataCommands, \
    NodeManagerCommands, SessionCommands
from eggroll.core.conf_keys import ClusterManagerConfKeys, \
    NodeManagerConfKeys, CoreConfKeys
from eggroll.core.constants import SerdesTypes
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint, ErServerNode, ErServerCluster, \
    ErProcessor, ErStoreList
from eggroll.core.meta_model import ErStore, ErSessionMeta
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.utils import _to_proto_string, _map_and_listify
from eggroll.core.utils import get_static_er_conf
from eggroll.core.utils import time_now_ns
from eggroll.utils.log_utils import get_logger

L = get_logger()


class CommandCallError(Exception):
    def __init__(self, command_uri: CommandURI, endpoint: ErEndpoint, *args: object) -> None:
        super().__init__(f"Failed to call command: {command_uri} to endpoint: {endpoint}, caused by: ", *args)


class CommandClient(object):
    _executor_pool = None
    _executor_pool_lock = threading.Lock()

    def __init__(self):
        self._channel_factory = GrpcChannelFactory()
        if CommandClient._executor_pool is None:
            with CommandClient._executor_pool_lock:
                if CommandClient._executor_pool is None:
                    _executor_pool_type = CoreConfKeys.EGGROLL_CORE_DEFAULT_EXECUTOR_POOL.get()
                    _max_workers = int(CoreConfKeys
                                       .EGGROLL_CORE_CLIENT_COMMAND_EXECUTOR_POOL_MAX_SIZE
                                       .get())
                    CommandClient._executor_pool = create_executor_pool(
                            canonical_name=_executor_pool_type,
                            max_workers=_max_workers,
                            thread_name_prefix="command_client")

    def simple_sync_send(self, input: RpcMessage, output_type, endpoint: ErEndpoint, command_uri: CommandURI, serdes_type=SerdesTypes.PROTOBUF):
        results = self.sync_send(inputs=[input], output_types=[output_type], endpoint=endpoint, command_uri=command_uri, serdes_type=serdes_type)

        if len(results):
            return results[0]
        else:
            return None

    def sync_send(self, inputs: list, output_types: list, endpoint: ErEndpoint, command_uri: CommandURI, serdes_type=SerdesTypes.PROTOBUF):
        request = None
        try:
            request = ErCommandRequest(id=time_now_ns(),
                                       uri=command_uri._uri,
                                       args=_map_and_listify(_to_proto_string, inputs))
            start = time.time()
            L.trace(f"[CC] calling: {endpoint} {command_uri} {request}")
            _channel = self._channel_factory.create_channel(endpoint)
            _command_stub = command_pb2_grpc.CommandServiceStub(_channel)
            response = _command_stub.call(request.to_proto())
            er_response = ErCommandResponse.from_proto(response)
            elapsed = time.time() - start
            L.trace(f"[CC] called (elapsed={elapsed}): {endpoint}, {command_uri}, {request}, {er_response}")
            byte_results = er_response._results

            if len(byte_results):
                zipped = zip(output_types, byte_results)
                return list(map(lambda t: t[0].from_proto_string(t[1]) if t[1] is not None else None, zipped))
            else:
                return []
        except Exception as e:
            L.exception(f'Error calling to {endpoint}, command_uri: {command_uri}, req:{request}')
            raise CommandCallError(command_uri, endpoint, e)

    def async_call(self, args, output_types: list, command_uri: CommandURI, serdes_type=SerdesTypes.PROTOBUF, callback=None):
        futures = list()
        for inputs, endpoint in args:
            f = self._executor_pool.submit(self.sync_send, inputs=inputs, output_types=output_types, endpoint=endpoint, command_uri=command_uri, serdes_type=serdes_type)
            if callback:
                f.add_done_callback(callback)
            futures.append(f)

        return futures


class ClusterManagerClient(object):
    def __init__(self, options=None):
        if options is None:
            options = {}
        static_er_conf = get_static_er_conf()
        host = options.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, static_er_conf.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, None))
        port = options.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, static_er_conf.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, None))

        if not host or not port:
            raise ValueError(f'failed to load host or port in creating cluster manager client. host: {host}, port: {port}')

        self.__endpoint = ErEndpoint(host, int(port))
        if 'serdes_type' in options:
            self.__serdes_type = options['serdes_type']
        else:
            self.__serdes_type = SerdesTypes.PROTOBUF
        self.__command_client = CommandClient()

    def get_server_node(self, input: ErServerNode):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErServerNode,
                command_uri=MetadataCommands.GET_SERVER_NODE,
                serdes_type=self.__serdes_type)

    def get_server_nodes(self, input: ErServerNode):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErServerCluster,
                command_uri=MetadataCommands.GET_SERVER_NODES,
                serdes_type=self.__serdes_type)

    def get_or_create_server_node(self, input: ErServerNode):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErServerNode,
                command_uri=MetadataCommands.GET_OR_CREATE_SERVER_NODE,
                serdes_type=self.__serdes_type)

    def create_or_update_server_node(self, input: ErServerNode):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErServerNode,
                command_uri=MetadataCommands.CREATE_OR_UPDATE_SERVER_NODE,
                serdes_type=self.__serdes_type)

    def get_store(self, input: ErStore):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErStore,
                command_uri=MetadataCommands.GET_STORE,
                serdes_type=self.__serdes_type)

    def get_or_create_store(self, input: ErStore):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErStore,
                command_uri=MetadataCommands.GET_OR_CREATE_STORE,
                serdes_type=self.__serdes_type)

    def delete_store(self, input: ErStore):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErStore,
                command_uri=MetadataCommands.DELETE_STORE,
                serdes_type=self.__serdes_type)

    def get_store_from_namespace(self, input):
        return self.__do_sync_request_internal(
            input=input,
            output_type=ErStoreList,
            command_uri=MetadataCommands.GET_STORE_FROM_NAMESPACE,
            serdes_type=self.__serdes_type)

    def get_or_create_session(self, input: ErSessionMeta):
        return self.__check_processors(
                self.__do_sync_request_internal(input=input,
                                                output_type=ErSessionMeta,
                                                command_uri=SessionCommands.GET_OR_CREATE_SESSION,
                                                serdes_type=self.__serdes_type))

    def register_session(self, session_meta: ErSessionMeta):
        return self.__check_processors(
                self.__command_client.sync_send(inputs=[session_meta],
                                                output_types=[ErSessionMeta],
                                                endpoint=self.__endpoint,
                                                command_uri=SessionCommands.REGISTER_SESSION,
                                                serdes_type=self.__serdes_type)[0])

    def get_session_server_nodes(self, input: ErSessionMeta):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErServerCluster,
                command_uri=SessionCommands.GET_SESSION_SERVER_NODES,
                serdes_type=self.__serdes_type)

    def heartbeat(self, input: ErProcessor):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErProcessor,
                command_uri=SessionCommands.HEARTBEAT,
                serdes_type=self.__serdes_type)

    def stop_session(self, input: ErSessionMeta):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErSessionMeta,
                command_uri=SessionCommands.STOP_SESSION,
                serdes_type=self.__serdes_type)

    def kill_session(self, input: ErSessionMeta):
        return self.__do_sync_request_internal(
                input=input,
                output_type=ErSessionMeta,
                command_uri=SessionCommands.KILL_SESSION,
                serdes_type=self.__serdes_type)

    def kill_all_sessions(self):
        return self.__do_sync_request_internal(
                input=ErSessionMeta(),
                output_type=ErSessionMeta,
                command_uri=SessionCommands.KILL_ALL_SESSIONS,
                serdes_type=self.__serdes_type)

    def __do_sync_request_internal(self, input, output_type, command_uri, serdes_type):
        return self.__command_client.simple_sync_send(input=input,
                                                      output_type=output_type,
                                                      endpoint=self.__endpoint,
                                                      command_uri=command_uri,
                                                      serdes_type=serdes_type)

    def __check_processors(self, session_meta: ErSessionMeta):
        if not session_meta.is_processors_valid():
            raise ValueError(f"processor in session meta is not valid: {session_meta}")
        return session_meta


class NodeManagerClient(object):
    def __init__(self, options: dict = None):
        if options is None:
            options = {}
        self.__endpoint = ErEndpoint(options[NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST], int(options[NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT]))
        if 'serdes_type' in options:
            self.__serdes_type = options['serdes_type']
        else:
            self.__serdes_type = SerdesTypes.PROTOBUF
        self.__command_client = CommandClient()
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
                serdes_type=self.__serdes_type)

    def __do_sync_request_internal(self, input, output_type, command_uri, serdes_type):
        return self.__command_client.simple_sync_send(input=input,
                                                      output_type=output_type,
                                                      endpoint=self.__endpoint,
                                                      command_uri=command_uri,
                                                      serdes_type=serdes_type)