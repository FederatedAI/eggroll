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
  def __init__(self, id: int, tag: str = '', total_size=-1):
    self._id = id
    self._tag = tag
    self._total_size = total_size

  def to_proto(self):
    return transfer_pb2.TransferHeader(id=self._id, tag=self._tag,
                                       totalSize=self._total_size)

  @staticmethod
  def from_proto(pb_message):
    return ErTransferHeader(id=pb_message.id,
                            tag=pb_message.tag,
                            total_size=pb_message.totalSize)

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return f'ErTransferHeader(id={self._id}, tag={self._tag}, size={self._total_size})'


class ErBatch(RpcMessage):
  def __init__(self, header: ErTransferHeader, data):
    self._header = header
    self._data = data

  def to_proto(self):
    return transfer_pb2.Batch(header=self._header.to_proto(),
                              batchSize=len(self._data), data=self._data)

  @staticmethod
  def from_proto(pb_message):
    return ErBatch(header=ErTransferHeader.from_proto(pb_message.header),
                   data=pb_message.data)

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return f'ErBatch(header={repr(self._header)}, batch_size={len(self._data)}, data=***)'
