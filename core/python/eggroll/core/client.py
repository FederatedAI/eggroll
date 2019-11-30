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


from eggroll.core.meta_model import ErEndpoint, ErServerNode, ErServerCluster, ErProcessor, ErProcessorBatch
from eggroll.core.meta_model import ErStore, ErStoreLocator, ErSessionMeta
from eggroll.core.constants import SerdesTypes
from eggroll.core.command.commands import MetadataCommands, NodeManagerCommands
from eggroll.core.base_model import RpcMessage
from eggroll.core.command.command_model import CommandURI
from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.utils import time_now
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.conf_keys import NodeManagerConfKeys


class CommandClient(object):

  def __init__(self):
    self._channel_factory = GrpcChannelFactory()

  def simple_sync_send(self, input: RpcMessage, output_type, endpoint: ErEndpoint, command_uri: CommandURI, serdes_type = SerdesTypes.PROTOBUF):
    # todo: add serializer logic here
    request = ErCommandRequest(id = time_now(), uri = command_uri._uri, args = [input.to_proto_string()])

    _channel = self._channel_factory.create_channel(endpoint)
    _command_stub = command_pb2_grpc.CommandServiceStub(_channel)

    response = _command_stub.call(request.to_proto())

    er_response = ErCommandResponse.from_proto(response)

    byte_result = er_response._results[0]

    # todo: add deserializer logic here
    if len(byte_result):
      return output_type.from_proto_string(byte_result)
    else:
      return None


class ClusterManagerClient(object):

  def __init__(self, opts = {'cluster_manager_host': 'localhost', 'cluster_manager_port': 4670}):
    self.__endpoint = ErEndpoint(opts['cluster_manager_host'], opts['cluster_manager_port'])
    if 'serdes_type' in opts:
      self.__serdes_type = opts['serdes_type']
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

  def __do_sync_request_internal(self, input, output_type, command_uri, serdes_type):
    return self.__command_client.simple_sync_send(input=input,
                                                  output_type=output_type,
                                                  endpoint=self.__endpoint,
                                                  command_uri=command_uri,
                                                  serdes_type=serdes_type)


class NodeManagerClient(object):
  def __init__(self, options = {NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST: 'localhost', NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT: 9394}):
    self.__endpoint = ErEndpoint(options[NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST], int(options[NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT]))
    if 'serdes_type' in options:
      self.__serdes_type = options['serdes_type']
    else:
      self.__serdes_type = SerdesTypes.PROTOBUF
    self.__command_client = CommandClient()

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