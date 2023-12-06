from datetime import timedelta

from google.protobuf.duration_pb2 import Duration
from torch.distributed import Store

from eggroll.config import Config
from eggroll.core.command.command_uri import RendezvousStoreCommands
from eggroll.core.proto import deepspeed_pb2
from ..client import BaseClient


class EggrollStore(Store):
    def __init__(
        self,
        config: Config,
        host,
        port,
        prefix,
        timeout: timedelta = timedelta(hours=24),
    ):
        self._config = config
        self._prefix = prefix
        self._timeout = timeout
        super().__init__()

        self._client = BaseClient(config=config, host=host, port=port)

    def get(self, key, timeout: timedelta = None):
        if isinstance(key, str):
            key = key.encode()
        if timeout is None:
            timeout = self._timeout
        seconds = int(timeout.total_seconds())
        nanos = int((timeout - timedelta(seconds=seconds)).microseconds * 1000)
        response = self._client.do_sync_request(
            input=deepspeed_pb2.StoreGetRequest(
                prefix=self._prefix,
                key=key,
                timeout=Duration(seconds=seconds, nanos=nanos),
            ),
            output_type=deepspeed_pb2.StoreGetResponse,
            command_uri=RendezvousStoreCommands.GET,
        )
        if response.is_timeout:
            raise RuntimeError("Socket Timeout")
        return response.value

    def set(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        return self._client.do_sync_request(
            deepspeed_pb2.StoreSetRequest(prefix=self._prefix, key=key, value=value),
            output_type=deepspeed_pb2.StoreSetResponse,
            command_uri=RendezvousStoreCommands.SET,
        )

    def add(self, key, amount):
        if isinstance(key, str):
            key = key.encode()
        response = self._client.do_sync_request(
            deepspeed_pb2.StoreAddRequest(prefix=self._prefix, key=key, amount=amount),
            output_type=deepspeed_pb2.StoreAddResponse,
            command_uri=RendezvousStoreCommands.ADD,
        )
        return response.amount

    def destroy(self):
        return destroy(self._client, self._prefix)


def destroy(client, prefix):
    return client.do_sync_request(
        deepspeed_pb2.StoreDestroyRequest(prefix=prefix),
        output_type=deepspeed_pb2.StoreDestroyResponse,
        command_uri=RendezvousStoreCommands.DESTROY,
    )
