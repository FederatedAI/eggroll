from datetime import timedelta
from torch.distributed import Store
from ..client import BaseClient
from .commands import RendezvousStoreCommands
from .model import RendezvousStoreSetRequest, RendezvousStoreSetResponse, RendezvousStoreGetRequest, \
    RendezvousStoreGetResponse, RendezvousStoreAddRequest, RendezvousStoreAddResponse


class EggrollStore(Store):
    def __init__(self, host, port, prefix, timeout: timedelta = timedelta(seconds=300)):
        self._host = host
        self._port = port
        self._prefix = prefix
        self._timeout = timeout
        super().__init__()

    def get(self, key, timeout: timedelta = None):
        if isinstance(key, str):
            key = key.encode()
        if timeout is None:
            timeout = self._timeout
        deepspeed_client = BaseClient()
        get_request = RendezvousStoreGetRequest(prefix=self._prefix, key=key, timeout=timeout)
        response = deepspeed_client.do_sync_request_internal(get_request,
                                                             output_type=RendezvousStoreGetResponse,
                                                             command_uri=RendezvousStoreCommands.GET)
        return response.value

    def set(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        deepspeed_client = BaseClient()
        set_request = RendezvousStoreSetRequest(prefix=self._prefix, key=key, value=value)
        return deepspeed_client.do_sync_request_internal(set_request,
                                                         output_type=RendezvousStoreSetResponse,
                                                         command_uri=RendezvousStoreCommands.SET)

    def add(self, key, amount):
        if isinstance(key, str):
            key = key.encode()
        deepspeed_client = BaseClient()
        add_request = RendezvousStoreAddRequest(prefix=self._prefix, key=key, amount=amount)
        response = deepspeed_client.do_sync_request_internal(add_request,
                                                             output_type=RendezvousStoreAddResponse,
                                                             command_uri=RendezvousStoreCommands.ADD)
        return response.amount
