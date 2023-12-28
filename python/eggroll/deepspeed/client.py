import logging
import time
import typing

from eggroll.config import Config
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErCommandRequest, ErCommandResponse
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.proto import command_pb2_grpc, deepspeed_download_pb2_grpc
from eggroll.core.proto.deepspeed_download_pb2 import (
    DsDownloadRequest,
    DsDownloadResponse,
)

if typing.TYPE_CHECKING:
    from eggroll.core.command.command_uri import CommandURI
T = typing.TypeVar("T")

L = logging.getLogger(__name__)


class BaseClient:
    def __init__(
        self,
        config: Config,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
    ):
        if host is None:
            host = config.eggroll.resourcemanager.clustermanager.host
        if port is None:
            port = config.eggroll.resourcemanager.clustermanager.port
        self._config = config
        self._endpoint = ErEndpoint(host, int(port))
        self._channel_factory = GrpcChannelFactory()

    def do_sync_request(
        self, input, output_type: typing.Type[T], command_uri: "CommandURI"
    ) -> T:
        request = ErCommandRequest(
            uri=command_uri.uri, args=[input.SerializeToString()]
        )
        try:
            start = time.time()
            L.debug(f"[CC] calling: {self._endpoint} {command_uri} {request}")
            _channel = self._channel_factory.create_channel(
                config=self._config, endpoint=self._endpoint
            )
            _command_stub = command_pb2_grpc.CommandServiceStub(_channel)
            response = _command_stub.call(request.to_proto())
            er_response = ErCommandResponse.from_proto(response)
            elapsed = time.time() - start
            L.debug(
                f"[CC] called (elapsed={elapsed}): {self._endpoint}, {command_uri}, {request}, {er_response}"
            )
            byte_results = er_response._results
            if len(byte_results) > 0 and byte_results[0] is not None:
                pb_message = output_type()
                pb_message.ParseFromString(byte_results[0])
                return pb_message
            else:
                return None
        except Exception as e:
            L.exception(
                f"Error calling to {self._endpoint}, command_uri: {command_uri}, req:{request}"
            )
            raise Exception(f"failed to call {command_uri} to {self._endpoint}: {e}")

    def do_download(self, input: DsDownloadRequest) -> DsDownloadResponse:
        try:
            _channel = self._channel_factory.create_channel(
                config=self._config, endpoint=self._endpoint
            )
            _deepspeed_stub = deepspeed_download_pb2_grpc.DsDownloadServiceStub(
                _channel
            )
            response = _deepspeed_stub.download(input)
            return response
        except Exception as e:
            L.exception(
                f"Error calling to {self._endpoint}, download deepspeed , req:{input}"
            )
            raise e

    def do_download_stream(self, input: DsDownloadRequest):
        try:
            _channel = self._channel_factory.create_channel(
                config=self._config, endpoint=self._endpoint
            )
            _deepspeed_stub = deepspeed_download_pb2_grpc.DsDownloadServiceStub(
                _channel
            )
            size = 0
            rank_map = {}
            for response in _deepspeed_stub.download_by_split(input):
                size += len(response.data)
                if response.rank in rank_map:
                    rank_map[response.rank] += response.data
                else:
                    rank_map[response.rank] = response.data

            print("recive bytes ", size)

            return rank_map

        except Exception as e:
            L.exception(
                f"Error calling to {self._endpoint}, download deepspeed , req:{input}"
            )
            raise e

    @property
    def channel_factory(self):
        return self._channel_factory

    @property
    def endpoint(self):
        return self._endpoint
