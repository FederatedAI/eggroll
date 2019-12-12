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


from typing import Iterator, Generator
from struct import pack_into, unpack_from, calcsize

MAGIC_NUM = '46709394'.encode('utf-8')
PROTOCOL_VERSION = '00000001'.encode('utf-8')


class BinBatchReader(object):
  def __init__(self, pair_batch, options = {}):
    self.__buffer = pair_batch
    self.__offset = 0
    self.__batch_size = len(pair_batch)
    if not self.__batch_size:
      return

    _magic_num = self.read_bytes(len(MAGIC_NUM), include_size=False)
    if _magic_num != MAGIC_NUM:
      raise ValueError('magic num does not match')

    _protocol_version = self.read_bytes(len(PROTOCOL_VERSION), include_size=False)
    if _protocol_version != PROTOCOL_VERSION:
      raise ValueError('protocol version not suppoted')

    header_size = self.read_int()
    body_size = self.read_int()

    if self.__batch_size - self.__offset != body_size:
      raise ValueError('body size does not match len of body')

  def has_remaining(self):
    return self.__offset < self.__batch_size

  def get_offset(self):
    return 0 + self.__offset

  def __check_readable(self, offset, size):
    if self.__batch_size - offset - size < 0:
      raise IndexError(f'read bin row pair batch buffer overflow. remaining: {self.__batch_size - offset}, required: {size}')

  def __get_op_offset(self, offset):
    if not offset:
      return self.__offset
    else:
      return offset

  def __adjust_offset(self, offset, delta):
    if offset is None:
      self.__offset += delta

  def read_int(self, offset=None):
    op_offset = self.__get_op_offset(offset)
    value_size = 4

    self.__check_readable(op_offset, value_size)
    result = unpack_from('>i', self.__buffer, op_offset)
    self.__adjust_offset(offset, value_size)

    return result[0]

  def read_bytes(self, size, offset = None, include_size = False):
    op_offset = self.__get_op_offset(offset)

    final_size = size
    format = f'{size}s'
    if include_size:
      final_size = size + 4
      format = f'>i{format}'
    self.__check_readable(op_offset, final_size)
    result = unpack_from(format, self.__buffer, op_offset)
    self.__adjust_offset(offset, final_size)

    if include_size:
      return result
    else:
      return result[0]


class BinBatchWriter(object):
  def __init__(self, options = {}):
    self._stage = 1
    self.__buffer = None
    self.__offset = 0
    self.__buffer = options.get('buffer', None)
    self.__batch_size = options.get('batch_size', 1 << 20)

    if self.__buffer:
      self.__resettable = False
    else:
      self.__resettable = True

    self.__reset()

  def __reset(self):
    if not self.__resettable and self.__offset != 0:
      raise ValueError('Bin batch write is unresettable')

    if self._stage != 1:
      return

    if self.__resettable:
      self.__buffer = bytearray(batch_size)
      self.__offset = 0

    self.write_bytes(MAGIC_NUM, include_size=False)
    self.write_bytes(PROTOCOL_VERSION, include_size=False)
    self.write_int(0)
    self.write_int(0)

    self._stage = 2

  def get_offset(self):
    return 0 + self.__offset

  def __check_writable(self, offset, size):
    if self._stage != 2 and self.__resettable:
      self.__reset()

    if self.__batch_size - offset - size < 0:
      raise IndexError(f'write bin batch buffer overflow. remaining: {self.__batch_size - offset}, required: {size}')

  def __get_op_offset(self, offset):
    if offset is None:
      return self.__offset
    else:
      return offset

  def __adjust_offset(self, offset, delta):
    if offset is None:
      self.__offset += delta
    else:
      if offset + delta > self.__offset:
        self.__offset = offset + delta

  def write_int(self, value, offset=None):
    size = 4
    op_offset = self.__get_op_offset(offset)

    self.__check_writable(op_offset, size)
    pack_into('>i', self.__buffer, op_offset, value)
    self.__adjust_offset(offset, size)

  def write_bytes(self, value, offset=None, include_size=False):
    value_size = len(value)
    format = f'{value_size}s'
    if include_size:
      format = f'>i{format}'
    size = calcsize(format)

    op_offset = self.__get_op_offset(offset)
    self.__check_writable(op_offset, size)
    if include_size:
      pack_into(format, self.__buffer, op_offset, value_size, value)
    else:
      pack_into(format, self.__buffer, op_offset, value)

    self.__adjust_offset(offset, size)

  def get_batch(self, end=None):
    body_size_offset = 16
    final_end = end if end else self.__offset
    body_size = final_end - body_size_offset - 4
    self.write_int(body_size, body_size_offset)
    mv = memoryview(self.__buffer)
    return bytes(mv[0:final_end])

