from eggroll.core.base_model import RpcMessage
from eggroll.core.proto import store_pb2
from datetime import timedelta
from google.protobuf.duration_pb2 import Duration


class RendezvousStoreSetRequest(RpcMessage):
    def __init__(self, prefix, key, value):
        self.prefix = prefix
        self.key = key
        self.value = value

    def to_proto(self):
        return store_pb2.SetRequest(
            prefix=self.prefix,
            key=self.key,
            value=self.value,
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: store_pb2.SetRequest):
        return RendezvousStoreSetRequest(prefix=pb_message.prefix, key=pb_message.key, value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = store_pb2.SetRequest()
        pb_message.ParseFromString(pb_string)
        return RendezvousStoreSetRequest.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'RendezvousStoreSetRequest(prefix={self.prefix}, key={self.key}, value={self.value})'


class RendezvousStoreSetResponse(RpcMessage):
    def __init__(self):
        ...

    def to_proto(self):
        return store_pb2.Empty()

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: store_pb2.SetResponse):
        return RendezvousStoreSetResponse()

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = store_pb2.SetResponse()
        pb_message.ParseFromString(pb_string)
        return RendezvousStoreSetResponse.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'RendezvousStoreSetResponse()'


class RendezvousStoreGetRequest(RpcMessage):
    def __init__(self, prefix: str, key: bytes, timeout: timedelta):
        self.prefix = prefix
        self.key = key
        self.timeout = timeout

    def to_proto(self):
        seconds = int(self.timeout.total_seconds())
        nanos = int((self.timeout - timedelta(seconds=seconds)).microseconds * 1000)
        return store_pb2.GetRequest(
            prefix=self.prefix,
            key=self.key,
            timeout=Duration(seconds=seconds, nanos=nanos),
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: store_pb2.GetRequest):
        return RendezvousStoreGetRequest(prefix=pb_message.prefix, key=pb_message.key, timeout=pb_message.timeout)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = store_pb2.GetRequest()
        pb_message.ParseFromString(pb_string)
        return RendezvousStoreGetRequest.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'RendezvousStoreGetRequest(prefix={self.prefix}, key={self.key}, timeout={self.timeout})'


class RendezvousStoreGetResponse(RpcMessage):
    def __init__(self, value: bytes):
        self.value = value

    def to_proto(self):
        return store_pb2.GetResponse(
            value=self.value,
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: store_pb2.GetResponse):
        return RendezvousStoreGetResponse(value=pb_message.value)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = store_pb2.GetResponse()
        pb_message.ParseFromString(pb_string)
        return RendezvousStoreGetResponse.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'RendezvousStoreGetResponse(value={self.value})'


class RendezvousStoreAddRequest(RpcMessage):
    def __init__(self, prefix, key: bytes, amount: int):
        self.prefix = prefix
        self.key = key
        self.amount = amount

    def to_proto(self):
        return store_pb2.AddRequest(
            prefix=self.prefix,
            key=self.key,
            amount=self.amount,
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: store_pb2.AddRequest):
        return RendezvousStoreAddRequest(prefix=pb_message.prefix, key=pb_message.key, amount=pb_message.amount)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = store_pb2.AddRequest()
        pb_message.ParseFromString(pb_string)
        return RendezvousStoreAddRequest.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'RendezvousStoreAddRequest(prefix={self.prefix}, key={self.key}, amount={self.amount})'


class RendezvousStoreAddResponse(RpcMessage):
    def __init__(self, amount: int):
        self.amount = amount

    def to_proto(self):
        return store_pb2.AddResponse()

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: store_pb2.AddResponse):
        return RendezvousStoreAddResponse(amount=pb_message.amount)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = store_pb2.AddResponse()
        pb_message.ParseFromString(pb_string)
        return RendezvousStoreAddResponse.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'AddResponse(amount={self.amount})'
