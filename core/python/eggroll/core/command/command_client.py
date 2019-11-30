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

from eggroll.core.base_model import RpcMessage
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.constants import SerdesTypes
from eggroll.core.command.command_model import CommandURI
from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.utils import time_now
from eggroll.core.grpc.factory import GrpcChannelFactory

# todo: move to core/client
class CommandClient(object):

  def __init__(self):
    self._channel_factory = GrpcChannelFactory()

  def simple_sync_send(self, input: RpcMessage, output_type, endpoint: ErEndpoint, command_uri: CommandURI, serdes_type = SerdesTypes.PROTOBUF):
    # todo: add serializer logic here
    request = ErCommandRequest(id=time_now(), uri=command_uri._uri, args=[input.to_proto_string()])

    _channel = self._channel_factory.create_channel(endpoint)
    _command_stub = command_pb2_grpc.CommandServiceStub(_channel)

    response = _command_stub.call(request.to_proto())

    er_response = ErCommandResponse.from_proto(response)

    byte_result = er_response._results[0]
    print(byte_result)

    # todo: add deserializer logic here
    if len(byte_result):
      return output_type.from_proto_string(byte_result)
    else:
      return None
