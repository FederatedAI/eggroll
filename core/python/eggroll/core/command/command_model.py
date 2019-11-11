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
from eggroll.core.proto import command_pb2
from urllib.parse import urlparse, parse_qs


class ErCommandRequest(RpcMessage):
  def __init__(self, id, uri: str, args=list(), kwargs=dict()):
    self._id = id
    self._uri = uri
    self._args = args
    self._kwargs = kwargs

  def to_proto(self):
    return command_pb2.CommandRequest(id=self._id, uri=self._uri,
                                      args=self._args, kwargs=self._kwargs)

  @staticmethod
  def from_proto(pb_message):
    return ErCommandRequest(id=pb_message.id,
                            uri=pb_message.uri,
                            args=pb_message.args,
                            kwargs=pb_message.kwargs)

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return f'ErCommandRequest(id={self._id}, uri={self._uri}, args=[***, len={len(self._args)}], kwargs=[***, len={len(self._kwargs)}])'


class ErCommandResponse(RpcMessage):
  def __init__(self, id, request: ErCommandRequest = None, results=list()):
    self._id = id
    self._request = request
    self._results = results

  def to_proto(self):
    return command_pb2.CommandResponse(id=self._id,
                                       request=self._request.to_proto() if self._request else None,
                                       results=self._results)

  @staticmethod
  def from_proto(pb_message):
    return ErCommandResponse(id=pb_message.id,
                             request=ErCommandRequest.from_proto(
                                 pb_message.request),
                             results=pb_message.results)

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return f'ErCommandResponse(id={self._id}, request={repr(self._request)}, results=(***))'


class CommandURI(object):
  def __init__(self, uri_string='', command_request=None):
    if command_request:
      uri_string = getattr(command_request, "_uri")

    self._uri = uri_string
    self._parse_result = urlparse(self._uri)
    self._query_string = self._parse_result.query
    self._query_pairs = parse_qs(
      self._query_string)  # dict of (key: str, [value: str])

    if not self._query_pairs:
      self._query_pairs['route'] = uri_string

  def get_query_value(self, key: str):
    value = self._query_pairs[key]
    if value:
      return value

  def get_route(self):
    return self.get_query_value('route')
