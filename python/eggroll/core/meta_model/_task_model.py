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

from ._base_model import RpcMessage
from eggroll.core.proto import egg_pb2


class CountResponse(RpcMessage):
    def __init__(self, value: int):
        self._value = value

    @property
    def value(self):
        return self._value

    def to_proto(self):
        return egg_pb2.CountResponse(value=self._value)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return CountResponse(value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.CountResponse()
        pb_message.ParseFromString(pb_string)
        return CountResponse.from_proto(pb_message)

    def __repr__(self):
        return f"<CountRequest(count={self._value}) at {hex(id(self))}>"


class GetRequest(RpcMessage):
    def __init__(self, key: bytes):
        self._key = key

    @property
    def key(self):
        return self._key

    def to_proto(self):
        return egg_pb2.GetRequest(key=self._key)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return GetRequest(key=pb_message.key)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.GetRequest()
        pb_message.ParseFromString(pb_string)
        return GetRequest.from_proto(pb_message)

    def __repr__(self):
        return f"<GetRequest(key={self._key}) at {hex(id(self))}>"


class GetResponse(RpcMessage):
    def __init__(self, key: bytes, value: bytes, exists: bool):
        self._key = key
        self._value = value
        self._exists = exists

    @property
    def value(self):
        return self._value

    @property
    def key(self):
        return self._key

    @property
    def exists(self):
        return self._exists

    def to_proto(self):
        return egg_pb2.GetResponse(
            key=self._key, value=self._value, exists=self._exists
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return GetResponse(
            key=pb_message.key, value=pb_message.value, exists=pb_message.exists
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.GetResponse()
        pb_message.ParseFromString(pb_string)
        return GetResponse.from_proto(pb_message)

    def __repr__(self):
        return f"<GetResponse(key={self._key}, value={self._value}, exist={self._exists}) at {hex(id(self))}>"


class PutRequest(RpcMessage):
    def __init__(self, key: bytes, value: bytes):
        self._key = key
        self._value = value

    @property
    def value(self):
        return self._value

    @property
    def key(self):
        return self._key

    def to_proto(self):
        return egg_pb2.PutRequest(key=self._key, value=self._value)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return PutRequest(key=pb_message.key, value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.PutRequest()
        pb_message.ParseFromString(pb_string)
        return PutRequest.from_proto(pb_message)

    def __repr__(self):
        return f"<PutRequest(key={self._key}, value={self._value}) at {hex(id(self))}>"


class PutResponse(RpcMessage):
    def __init__(self, key: bytes, value: bytes, success: bool):
        self._key = key
        self._value = value
        self._success = success

    @property
    def value(self):
        return self._value

    @property
    def key(self):
        return self._key

    @property
    def success(self):
        return self._success

    def to_proto(self):
        return egg_pb2.PutResponse(
            key=self._key, value=self._value, success=self._success
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return PutResponse(
            key=pb_message.key, value=pb_message.value, success=pb_message.success
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.PutResponse()
        pb_message.ParseFromString(pb_string)
        return PutResponse.from_proto(pb_message)

    def __repr__(self):
        return f"<PutResponse(key={self._key}, value={self._value}, success={self._success}) at {hex(id(self))}>"


class GetAllRequest(RpcMessage):
    def __init__(self, limit: int):
        self._limit = limit

    @property
    def limit(self):
        return self._limit

    def to_proto(self):
        return egg_pb2.GetAllRequest(limit=self._limit)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return GetAllRequest(limit=pb_message.limit)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.GetAllRequest()
        pb_message.ParseFromString(pb_string)
        return GetAllRequest.from_proto(pb_message)

    def __repr__(self):
        return f"<GetAllRequest(limit={self._limit}) at {hex(id(self))}>"


class DeleteRequest(RpcMessage):
    def __init__(self, key: bytes):
        self._key = key

    @property
    def key(self):
        return self._key

    def to_proto(self):
        return egg_pb2.DeleteRequest(key=self._key)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return DeleteRequest(key=pb_message.key)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.DeleteRequest()
        pb_message.ParseFromString(pb_string)
        return DeleteRequest.from_proto(pb_message)

    def __repr__(self):
        return f"<DeleteRequest(key={self._key}) at {hex(id(self))}>"


class DeleteResponse(RpcMessage):
    def __init__(self, key: bytes, success: bool):
        self._key = key
        self._success = success

    @property
    def key(self):
        return self._key

    @property
    def success(self):
        return self._success

    def to_proto(self):
        return egg_pb2.DeleteResponse(key=self._key, success=self._success)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return DeleteResponse(key=pb_message.key, success=pb_message.success)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.DeleteResponse()
        pb_message.ParseFromString(pb_string)
        return DeleteResponse.from_proto(pb_message)

    def __repr__(self):
        return f"<DeleteResponse(key={self._key}, success={self._success}) at {hex(id(self))}>"


class MapPartitionsWithIndexRequest(RpcMessage):
    def __init__(self, shuffle: bool):
        self._shuffle = shuffle

    @property
    def shuffle(self):
        return self._shuffle

    def to_proto(self):
        return egg_pb2.MapPartitionsWithIndexRequest(shuffle=self._shuffle)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return MapPartitionsWithIndexRequest(shuffle=pb_message.shuffle)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.MapPartitionsWithIndexRequest()
        pb_message.ParseFromString(pb_string)
        return MapPartitionsWithIndexRequest.from_proto(pb_message)

    def __repr__(self):
        return f"<MapPartitionsWithIndexRequest(shuffle={self._shuffle}) at {hex(id(self))}>"


class ReduceResponse(RpcMessage):
    def __init__(self, id: int, value: bytes):
        self._id = id
        self._value = value

    @property
    def id(self):
        return self._id

    @property
    def value(self):
        return self._value

    def to_proto(self):
        return egg_pb2.ReduceResponse(id=self._id, value=self._value)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ReduceResponse(id=pb_message.id, value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.ReduceResponse()
        pb_message.ParseFromString(pb_string)
        return ReduceResponse.from_proto(pb_message)

    def __repr__(self):
        return (
            f"<ReduceResponse(id={self._id}, value={self._value}) at {hex(id(self))}>"
        )


class AggregateRequest(RpcMessage):
    def __init__(self, zero_value: bytes):
        self._zero_value = zero_value

    @property
    def zero_value(self):
        return None if not self._zero_value else self._zero_value

    def to_proto(self):
        return egg_pb2.AggregateRequest(zero_value=self._zero_value)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return AggregateRequest(zero_value=pb_message.zero_value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.AggregateRequest()
        pb_message.ParseFromString(pb_string)
        return AggregateRequest.from_proto(pb_message)

    def __repr__(self):
        return f"<AggregateRequest(zero_value={self._zero_value}) at {hex(id(self))}>"


class AggregateResponse(RpcMessage):
    def __init__(self, id: int, value: bytes):
        self._id = id
        self._value = value

    @property
    def value(self):
        return self._value

    def to_proto(self):
        return egg_pb2.AggregateResponse(id=self._id, value=self._value)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return AggregateResponse(id=pb_message.id, value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.AggregateResponse()
        pb_message.ParseFromString(pb_string)
        return AggregateResponse.from_proto(pb_message)

    def __repr__(self):
        return f"<AggregateResponse(id={self._id}, value={self._value}) at {hex(id(self))}>"


class WithStoresResponse(RpcMessage):
    def __init__(self, id: int, value: bytes):
        self._id = id
        self._value = value

    @property
    def value(self):
        return self._value

    @property
    def id(self):
        return self._id

    def to_proto(self):
        return egg_pb2.WithStoresResponse(id=self._id, value=self._value)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return WithStoresResponse(id=pb_message.id, value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = egg_pb2.WithStoresResponse()
        pb_message.ParseFromString(pb_string)
        return WithStoresResponse.from_proto(pb_message)

    def __repr__(self):
        return f"<WithStoresResponse(id={self._id}, value={self._value}) at {hex(id(self))}>"
