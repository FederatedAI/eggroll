import logging
import time
import typing

from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse
from eggroll.core.conf_keys import ClusterManagerConfKeys
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.proto import command_pb2_grpc, deepspeed_download_pb2_grpc
from eggroll.core.proto.deepspeed_download_pb2 import DsDownloadRequest, DsDownloadResponse
from eggroll.core.utils import get_static_er_conf, time_now_ns

T = typing.TypeVar("T")

L = logging.getLogger(__name__)


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
                f"failed to load host or port in creating cluster manager client. host: {host}, port: {port}"
            )

        self._endpoint = ErEndpoint(host, int(port))
        self._channel_factory = GrpcChannelFactory()

    def do_sync_request(self, input, output_type: typing.Type[T], command_uri) -> T:
        request = ErCommandRequest(id=time_now_ns(), uri=command_uri._uri, args=[input.SerializeToString()])
        try:
            start = time.time()
            L.debug(f"[CC] calling: {self._endpoint} {command_uri} {request}")
            _channel = self._channel_factory.create_channel(self._endpoint)
            _command_stub = command_pb2_grpc.CommandServiceStub(_channel)
            response = _command_stub.call(request.to_proto())
            er_response = ErCommandResponse.from_proto(response)
            elapsed = time.time() - start
            L.debug(f"[CC] called (elapsed={elapsed}): {self._endpoint}, {command_uri}, {request}, {er_response}")
            byte_results = er_response._results
            if len(byte_results) > 0 and byte_results[0] is not None:
                pb_message = output_type()
                pb_message.ParseFromString(byte_results[0])
                return pb_message
            else:
                return None
        except Exception as e:
            L.exception(f"Error calling to {self._endpoint}, command_uri: {command_uri}, req:{request}")
            raise Exception(f"failed to call {command_uri} to {self._endpoint}: {e}")


    def  do_download(self,input: DsDownloadRequest)->DsDownloadResponse :
        try:
            start = time.time()
            _channel = self._channel_factory.create_channel(self._endpoint)
            _deepspped_stub = deepspeed_download_pb2_grpc.DsDownloadServiceStub(_channel)
            response = _deepspped_stub.download(input)
            return  response
        except Exception as e:

            L.exception(f"Error calling to {self._endpoint}, download deepspeed , req:{input}")
            raise e