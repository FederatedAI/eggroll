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

from eggroll.core.command.command_model import ErCommandRequest, \
    ErCommandResponse, CommandURI
from eggroll.core.command.command_router import CommandRouter
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.utils import _exception_logger


class CommandServicer(command_pb2_grpc.CommandServiceServicer):
    @_exception_logger
    def call(self, request, context):
        command_request = ErCommandRequest.from_proto(request)

        command_uri = CommandURI(command_request=command_request)

        service_name = command_uri.get_route()
        call_result = CommandRouter.get_instance() \
            .dispatch(service_name=service_name,
                      args=getattr(command_request, '_args'),
                      kwargs=getattr(command_request, '_kwargs'))

        response = ErCommandResponse(id=getattr(command_request, '_id'),
                                     results=call_result)

        return response.to_proto()
