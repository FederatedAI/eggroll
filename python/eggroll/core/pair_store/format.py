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


import os
from struct import pack_into, unpack_from, unpack, pack

MAGIC_NUM = bytes.fromhex('46709394')
PROTOCOL_VERSION = bytes.fromhex('00000001')

# def create_byte_buffer(data, options=None):
#     if options and "buffer_type" in options :
#         if options["buffer_type"] == "array":
#             return ArrayByteBuffer(data)
#         elif options["buffer_type"] != "file":
#             return FileByteBuffer(data)
#         else:
#             raise ValueError("not supported:", options)
#     return ArrayByteBuffer(data)


class ByteBuffer:
    def remaining_size(self):
        return self.size() - self.get_offset()

    def _check_remaining(self, offset, size):
        raise NotImplementedError()

    def size(self):
        raise NotImplementedError()

    def get_offset(self):
        raise NotImplementedError()

    def set_offset(self, offset):
        raise NotImplementedError()

    def read_int32(self, offset=None):
        raise NotImplementedError()

    def read_bytes(self, size, offset=None):
        raise NotImplementedError()

    def write_int32(self, value, offset=None):
        raise NotImplementedError()

    def write_bytes(self, value, offset=None):
        raise NotImplementedError()


class FileByteBuffer:
    def __init__(self, file):
        if not "b" in file.mode:
            raise ValueError("file is not binary mode:" + file.name)
        self.file = file
        # TODO:1: cached?
        self.__size = self.size()

    def remaining_size(self):
        return self.size() - self.get_offset()

    def _check_remaining(self, offset, size):
        if offset is None:
            offset = self.get_offset()
        if self.size() - offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.__size - offset}, required: {size}')

    def size(self):
        return os.fstat(self.file.fileno()).st_size

    def __seek_offset(self, offset):
        if offset is not None:
            self.file.seek(offset)

    def get_offset(self):
        return self.file.tell()

    def set_offset(self, offset):
        self.file.seek(offset)

    def read_int32(self, offset=None):
        self._check_remaining(offset, 4)
        self.__seek_offset(offset)
        return unpack(">i", self.file.read(4))[0]

    def read_bytes(self, size, offset=None):
        self._check_remaining(offset, size)
        self.__seek_offset(offset)
        return self.file.read(size)

    def write_int32(self, value, offset=None):
        self.__seek_offset(offset)
        self.file.write(pack(">i", value))

    def write_bytes(self, value, offset=None):
        self.__seek_offset(offset)
        self.file.write(value)


class ArrayByteBuffer(ByteBuffer):
    def __init__(self, data):
        self.__buffer = data
        self.__offset = 0
        self.__size = len(data)

    def get_offset(self):
        return self.__offset

    def set_offset(self, offset):
        self.__offset = offset

    def size(self):
        return self.__size

    def __get_op_offset(self, offset):
        if offset is None:
            return self.__offset
        else:
            return offset

    def __adjust_offset(self, offset, delta):
        self.__offset = offset + delta

    def read_int32(self, offset=None):
        op_offset = self.__get_op_offset(offset)
        value_size = 4
        self._check_remaining(op_offset, value_size)
        result = unpack_from('>i', self.__buffer, op_offset)
        self.__adjust_offset(op_offset, value_size)
        return result[0]

    def _check_remaining(self, offset, size):
        if self.__size - offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - offset}, required: {size}')

    def read_bytes(self, size, offset=None):
        op_offset = self.__get_op_offset(offset)
        self._check_remaining(op_offset, size)
        ret = self.__buffer[op_offset: op_offset + size]
        self.__adjust_offset(op_offset, size)
        return ret

    def write_int32(self, value, offset=None):
        size = 4
        op_offset = self.__get_op_offset(offset)
        self._check_remaining(op_offset, size)
        pack_into('>i', self.__buffer, op_offset, value)
        self.__adjust_offset(op_offset, size)

    def write_bytes(self, value, offset=None):
        op_offset = self.__get_op_offset(offset)
        size = len(value)
        self._check_remaining(op_offset, size)
        self.__buffer[op_offset: op_offset + size] = value
        self.__adjust_offset(op_offset, size)


class PairBinReader(object):
    def __init__(self, pair_buffer, data=None):
        if data is None:
            self.use_array_byte_buffer = True
            self.__buf = pair_buffer
            _magic_num = self.__buf.read_bytes(len(MAGIC_NUM))
            if _magic_num != MAGIC_NUM:
                raise ValueError('magic num does not match')

            _protocol_version = self.__buf.read_bytes(len(PROTOCOL_VERSION))
            if _protocol_version != PROTOCOL_VERSION:
                raise ValueError('protocol version not suppoted')
            # move offset, do not delete
            header_size = self.__buf.read_int32()
            body_size = self.__buf.read_int32()

            if body_size > 0 and self.__buf.size() - self.__buf.get_offset() != body_size:
                raise ValueError('body size does not match len of body')
        else:
            self.use_array_byte_buffer = False
            self.__data = data
            self.__size = len(data)
            self.__offset = 0

            # _magic_num = self.read_bytes(len(MAGIC_NUM))
            if self.__size - self.__offset - len(MAGIC_NUM) < 0:
                raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {len(MAGIC_NUM)}')
            _magic_num = self.__data[self.__offset: self.__offset + len(MAGIC_NUM)]
            self.__offset = self.__offset + len(MAGIC_NUM)

            if _magic_num != MAGIC_NUM:
                raise ValueError('magic num does not match')

            # _protocol_version = self.read_bytes(len(PROTOCOL_VERSION))
            if self.__size - self.__offset - len(PROTOCOL_VERSION) < 0:
                raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {len(PROTOCOL_VERSION)}')
            _protocol_version = self.__data[self.__offset: self.__offset + len(PROTOCOL_VERSION)]
            self.__offset = self.__offset + len(PROTOCOL_VERSION)
            if _protocol_version != PROTOCOL_VERSION:
                raise ValueError('protocol version not suppoted')

            # move offset, do not delete
            # header_size = self.read_int32()
            value_size = 4
            if self.__size - self.__offset - value_size < 0:
                raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {value_size}')
            header_size = unpack_from('>i', self.__data, self.__offset)[0]
            self.__offset = self.__offset + value_size

            # body_size = self.read_int32()
            value_size = 4
            if self.__size - self.__offset - value_size < 0:
                raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {value_size}')
            body_size = unpack_from('>i', self.__data, self.__offset)[0]
            self.__offset = self.__offset + value_size

            if body_size > 0 and self.size() - self.get_offset() != body_size:
                raise ValueError('body size does not match len of body')

    def remaining_size(self):
        return self.size() - self.get_offset()

    def size(self):
        return self.__size

    def set_offset(self, offset):
        self.__offset = offset

    def get_offset(self):
        return self.__offset

    def __get_op_offset(self, offset):
        if offset is None:
            return self.__offset
        else:
            return offset

    def __adjust_offset(self, offset, delta):
        self.__offset = offset + delta

    def _check_remaining(self, offset, size):
        if self.__size - offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - offset}, required: {size}')

    def read_bytes(self, size, offset=None):
        if offset is None:
            op_offset = self.__offset
        else:
            op_offset = offset
        if self.__size - op_offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - op_offset}, required: {size}')
        ret = self.__data[op_offset: op_offset + size]
        self.__offset = op_offset + size
        return ret

    def read_int32(self, offset=None):
        if offset is None:
            op_offset = self.__offset
        else:
            op_offset = offset
        value_size = 4
        if self.__size - op_offset - value_size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - op_offset}, required: {value_size}')
        result = unpack_from('>i', self.__data, op_offset)
        self.__offset = op_offset + value_size
        return result[0]

    def write_int32(self, value, offset=None):
        size = 4
        # op_offset = self.__get_op_offset(offset)
        if offset is None:
            op_offset = self.__offset
        else:
            op_offset = offset
        # self._check_remaining(op_offset, size)
        if self.__size - op_offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - op_offset}, required: {size}')
        pack_into('>i', self.__data, op_offset, value)
        # self.__adjust_offset(op_offset, size)
        self.__offset = op_offset + size

    def read_all(self):
        if self.use_array_byte_buffer:
            while self.__buf.remaining_size() > 0:
                old_offset = self.__buf.get_offset()
                try:
                    key_size = self.__buf.read_int32()
                    # empty means end, though there is remaining data
                    if key_size == 0:
                        self.__buf.set_offset(old_offset)
                        return
                    key = self.__buf.read_bytes(size=key_size)
                    value_size = self.__buf.read_int32()
                    value = self.__buf.read_bytes(size=value_size)
                except IndexError as e:
                    # read end
                    self.__buf.set_offset(old_offset)
                    return
                yield key, value
        else:
            # while self.remaining_size() > 0:
            while self.__size - self.__offset > 0:
                # old_offset = self.get_offset()
                old_offset = self.__offset
                try:
                    # key_size = self.read_int32()
                    int_size = 4
                    if self.__size - self.__offset - int_size < 0:
                        raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {int_size}')
                    key_size = unpack_from('>i', self.__data, self.__offset)[0]
                    self.__offset = self.__offset + int_size

                    # empty means end, though there is remaining data
                    if key_size == 0:
                        # self.set_offset(old_offset)
                        self.__offset = old_offset
                        return

                    # key = self.read_bytes(size=key_size)
                    if self.__size - self.__offset - key_size < 0:
                        raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {key_size}')
                    key = self.__data[self.__offset: self.__offset + key_size]
                    self.__offset = self.__offset + key_size

                    #value_size = self.read_int32()
                    int_size = 4
                    if self.__size - self.__offset - int_size < 0:
                        raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {int_size}')
                    value_size = unpack_from('>i', self.__data, self.__offset)[0]
                    self.__offset = self.__offset + int_size

                    # value = self.read_bytes(size=value_size)
                    if self.__size - self.__offset - value_size < 0:
                        raise IndexError(f'buffer overflow. remaining: {self.__size - self.__offset}, required: {value_size}')
                    value = self.__data[self.__offset: self.__offset + value_size]
                    self.__offset = self.__offset + value_size
                    
                except IndexError as e:
                    # read end
                    self.__offset = old_offset
                    return
                yield key, value


class PairBinWriter(object):

    @staticmethod
    def write_pair(buf, key_bytes, value_bytes):
        old_offset = buf.get_offset()
        try:
            buf.write_int32(len(key_bytes))
            buf.write_bytes(key_bytes)
            buf.write_int32(len(value_bytes))
            buf.write_bytes(value_bytes)
        except IndexError as e:
            buf.set_offset(old_offset)
            raise e

    @staticmethod
    def write_head(buf):
        buf.write_bytes(MAGIC_NUM)
        buf.write_bytes(PROTOCOL_VERSION)
        buf.write_int32(0)
        buf.write_int32(0)

    def __init__(self, pair_buffer, data=None):
        if data is None:
            self.use_array_byte_buffer = True
            self.__buf = pair_buffer
            self.write_head(self.__buf)
        else:
            self.__data = data
            self.__size = len(data)
            self.__offset = 0

            # self.write_bytes(MAGIC_NUM)
            size = len(MAGIC_NUM)
            if self.__size - self.__offset - size < 0:
                raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
            self.__data[self.__offset: self.__offset + size] = MAGIC_NUM
            self.__offset = self.__offset + size

            # self.write_bytes(PROTOCOL_VERSION)
            size = len(PROTOCOL_VERSION)
            if self.__size - self.__offset - size < 0:
                raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
            self.__data[self.__offset: self.__offset + size] = PROTOCOL_VERSION
            self.__offset = self.__offset + size

            # self.write_int32(0)
            size = 4
            if self.__size - self.__offset - size < 0:
                raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
            pack_into('>i', self.__data, self.__offset, 0)
            self.__offset = self.__offset + size

            # self.write_int32(0)
            size = 4
            if self.__size - self.__offset - size < 0:
                raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
            pack_into('>i', self.__data, self.__offset, 0)
            self.__offset = self.__offset + size

            self.use_array_byte_buffer = False

    def size(self):
        return self.__size

    def get_offset(self):
        return self.__offset

    def set_offset(self, offset):
        self.__offset = offset

    def get_data(self):
        return self.__data

    def __get_op_offset(self, offset):
        if offset is None:
            return self.__offset
        else:
            return offset

    def __adjust_offset(self, offset, delta):
        self.__offset = offset + delta

    def _check_remaining(self, offset, size):
        if self.__size - offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - offset}, required: {size}')

    def write_int32(self, value, offset=None):
        size = 4
        if offset is None:
            op_offset = self.__offset
        else:
            op_offset = offset
        if self.__size - op_offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - op_offset}, required: {size}')
        pack_into('>i', self.__data, op_offset, value)
        self.__offset = op_offset + size

    def write_bytes(self, value, offset=None):
        if offset is None:
            op_offset = self.__offset
        else:
            op_offset = offset
        size = len(value)
        if self.__size - op_offset - size < 0:
            raise IndexError(f'buffer overflow. remaining: {self.size() - op_offset}, required: {size}')
        self.__data[op_offset: op_offset + size] = value
        self.__offset = op_offset + size

    def write(self, key_bytes, value_bytes):
        if self.use_array_byte_buffer:
            PairBinWriter.write_pair(self.__buf, key_bytes=key_bytes, value_bytes=value_bytes)
        else:
            old_offset = self.__offset
            try:
                # self.write_int32(len(key_bytes))
                size = 4
                if self.__size - self.__offset - size < 0:
                    raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
                pack_into('>i', self.__data, self.__offset, len(key_bytes))
                self.__offset = self.__offset + size

                # self.write_bytes(key_bytes)
                size = len(key_bytes)
                if self.__size - self.__offset - size < 0:
                    raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
                self.__data[self.__offset: self.__offset + size] = key_bytes
                self.__offset = self.__offset + size

                # self.write_int32(len(value_bytes))
                size = 4
                if self.__size - self.__offset - size < 0:
                    raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
                pack_into('>i', self.__data, self.__offset, len(value_bytes))
                self.__offset = self.__offset + size

                # self.write_bytes(value_bytes)
                size = len(value_bytes)
                if self.__size - self.__offset - size < 0:
                    raise IndexError(f'buffer overflow. remaining: {self.size() - self.__offset}, required: {size}')
                self.__data[self.__offset: self.__offset + size] = value_bytes
                self.__offset = self.__offset + size

            except IndexError as e:
                self.__offset = old_offset
                raise e

    def write_all(self, items):
        for k, v in items:
            self.write(k, v)
