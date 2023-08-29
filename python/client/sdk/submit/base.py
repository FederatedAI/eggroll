import logging
import time
import traceback
import typing

from ..core.command_model import ErCommandRequest, ErCommandResponse
from ..core.factory import GrpcChannelFactory
from ..core.meta_model import ErEndpoint
from ..core.proto import command_pb2_grpc,extend_pb2_grpc
from ..core.utils import time_now_ns

T = typing.TypeVar("T")

L = logging.getLogger(__name__)


class BaseClient:
    def __init__(self, host: str, port: int):
        self._endpoint = ErEndpoint(host, int(port))
        self._channel_factory = GrpcChannelFactory()

    def do_sync_request(self, input, output_type: typing.Type[T], command_uri) -> T:
        request = ErCommandRequest(id=time_now_ns(), uri=command_uri._uri, args=[input.SerializeToString()])
        try:
            start = time.time()
            _channel = self._channel_factory.create_channel(self._endpoint)
            _command_stub = command_pb2_grpc.CommandServiceStub(_channel)
            response = _command_stub.call(request.to_proto())
            er_response = ErCommandResponse.from_proto(response)
            elapsed = time.time() - start
            byte_results = er_response._results
            if len(byte_results) > 0 and byte_results[0] is not None:
                pb_message = output_type()
                pb_message.ParseFromString(byte_results[0])
                return pb_message
            else:
                return None
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"failed to call {command_uri} to {self._endpoint}: {e}")


    @property
    def channel_factory(self):
        return self._channel_factory

    @property
    def endpoint(self):
        return self._endpoint
