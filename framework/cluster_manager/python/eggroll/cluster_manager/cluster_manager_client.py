# -*- coding: utf-8 -*-
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

from eggroll.core.client import CommandClient
from eggroll.core.meta_model import ErEndpoint, ErServerNode, ErServerCluster
from eggroll.core.meta_model import ErStore, ErStoreLocator
from eggroll.core.constants import SerdesTypes
from eggroll.core.command.commands import MetadataCommands
from eggroll.core.base_model import RpcMessage
from eggroll.core.command.command_model import CommandURI
from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.utils import time_now
from eggroll.core.grpc.factory import GrpcChannelFactory



# todo: move to core/clients
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
