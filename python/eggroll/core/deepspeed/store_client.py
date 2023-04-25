from eggroll.core.client import ClusterManagerClient
from eggroll.core.deepspeed.store_model import RendezvousStoreSetRequest, RendezvousStoreGetRequest, \
    RendezvousStoreAddRequest
from datetime import timedelta
from torch.distributed import Store


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
        cluster_manager_client = ClusterManagerClient(options={})
        get_request = RendezvousStoreGetRequest(prefix=self._prefix, key=key, timeout=timeout)
        response = cluster_manager_client.rendezvous_store_get(get_request)
        return response.value

    def set(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        cluster_manager_client = ClusterManagerClient(options={})
        set_request = RendezvousStoreSetRequest(prefix=self._prefix, key=key, value=value)
        return cluster_manager_client.rendezvous_store_set(set_request)

    def add(self, key, amount):
        if isinstance(key, str):
            key = key.encode()
        cluster_manager_client = ClusterManagerClient(options={})
        add_request = RendezvousStoreAddRequest(prefix=self._prefix, key=key, amount=amount)
        response = cluster_manager_client.rendezvous_store_add(add_request)
        return response.amount
