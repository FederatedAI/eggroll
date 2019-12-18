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
from eggroll.core.proto import transfer_pb2


class ErTransferHeader(RpcMessage):
  def __init__(self, id: int, tag: str = '', total_size=-1, status=''):
    self._id = id
    self._tag = tag
    self._total_size = total_size
    self._status = status

  def to_proto(self):
    return transfer_pb2.TransferHeader(id=self._id,
                                       tag=self._tag,
                                       totalSize=self._total_size,
                                       status=self._status)

  @staticmethod
  def from_proto(pb_message):
    return ErTransferHeader(id=pb_message.id,
                            tag=pb_message.tag,
                            total_size=pb_message.totalSize,
                            status=pb_message.status)

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return f'ErTransferHeader(id={self._id}, tag={self._tag}, size={self._total_size}, status={self._status})'


class ErTransferBatch(RpcMessage):
  def __init__(self, header: ErTransferHeader, batch_size = -1, data = None):
    self._header = header
    self._batch_size = batch_size
    self._data = data

  def to_proto(self):
    return transfer_pb2.TransferBatch(header=self._header.to_proto(),
                                      batchSize=self._batch_size, data=self._data)

  @staticmethod
  def from_proto(pb_message):
    return ErTransferBatch(header=ErTransferHeader.from_proto(pb_message.header),
                           batch_size=pb_message.batchSize,
                           data=pb_message.data)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = transfer_pb2.TransferBatch()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErTransferBatch.from_proto(pb_message)


  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return f'ErTransferBatch(header={repr(self._header)}, batch_size={self._batch_size}, data=***)'
