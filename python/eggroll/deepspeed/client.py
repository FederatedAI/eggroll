from eggroll.core.meta_model import ErEndpoint
from eggroll.core.constants import SerdesTypes
from eggroll.core.client import CommandClient, get_static_er_conf, ClusterManagerConfKeys
import typing


class BaseClient:
    def __init__(self, host: typing.Optional[str] = None, port: typing.Optional[int] = None):
        if host is None or port is None:
            static_er_conf = get_static_er_conf()
            if host is None:
                host = static_er_conf.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, None)
            if port is None:
                port = static_er_conf.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, None)
        if not host or not port:
            raise ValueError(
                f'failed to load host or port in creating cluster manager client. host: {host}, port: {port}')

        self._endpoint = ErEndpoint(host, int(port))
        self._serdes_type = SerdesTypes.PROTOBUF
        self._command_client = CommandClient()

    def do_sync_request_internal(self, input, output_type, command_uri):
        return self._command_client.simple_sync_send(input=input,
                                                     output_type=output_type,
                                                     endpoint=self._endpoint,
                                                     command_uri=command_uri,
                                                     serdes_type=self._serdes_type)
