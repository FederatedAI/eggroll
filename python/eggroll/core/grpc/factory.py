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

from eggroll import trace
import ipaddress
import logging
import threading

import grpc

from eggroll.core.meta_model import ErEndpoint
from eggroll.config import Config

L = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def wrap_host_scheme(host):
    try:
        ip = ipaddress.ip_address(host)
        return f"ipv{ip.version}:{host}"
    except ValueError as e:
        return host


def _get_state(channel: grpc.Channel, try_to_connect=False):
    """
    helper function to get channel state, since similar function in grpc.aio.Channel is not exposed in grpc.Channel
    """
    result = channel._channel.check_connectivity_state(try_to_connect)
    return grpc._common.CYGRPC_CONNECTIVITY_STATE_TO_CHANNEL_CONNECTIVITY[result]


def _is_channel_shutdown(state):
    return state == grpc.ChannelConnectivity.SHUTDOWN


class GrpcChannelFactory(object):
    pool = {}
    _pool_lock = threading.Lock()

    @classmethod
    def create_channel(
        cls,
        config: Config,
        endpoint: ErEndpoint,
        is_secure_channel=False,
        refresh=False,
    ):
        target = endpoint.endpoint_str()
        with GrpcChannelFactory._pool_lock:
            if refresh or cls._should_refresh(target):
                old_channel = GrpcChannelFactory.pool.get(target, None)
                GrpcChannelFactory.pool[target] = cls._create_grpc_channel(
                    config, target
                )
                if old_channel is not None:
                    old_channel.close()
            return GrpcChannelFactory.pool[target]

    @classmethod
    def _should_refresh(cls, target):
        if (cached_channel := GrpcChannelFactory.pool.get(target, None)) is not None:
            cached_channel_state = _get_state(cached_channel)
            # L.debug(f"old channel to {target}'s status={cached_channel_state}")
            if not _is_channel_shutdown(cached_channel_state):
                return False
        return True

    @staticmethod
    def shutdown_all_now():
        for target, channel in GrpcChannelFactory.pool.items():
            L.debug(f"start to shutdown channel={channel}, target={target}")
            channel.close()

    @classmethod
    def _create_grpc_channel(cls, config: Config, target):
        return grpc.insecure_channel(
            target=target,
            options=[
                (
                    "grpc.max_send_message_length",
                    config.eggroll.core.grpc.channel.max.inbound.message.size,
                ),
                (
                    "grpc.max_receive_message_length",
                    config.eggroll.core.grpc.channel.max.inbound.message.size,
                ),
                (
                    "grpc.max_metadata_size",
                    config.eggroll.core.grpc.channel.max.inbound.metadata.size,
                ),
                (
                    "grpc.keepalive_time_ms",
                    config.eggroll.core.grpc.channel.keepalive.time.sec * 1000,
                ),
                (
                    "grpc.keepalive_timeout_ms",
                    config.eggroll.core.grpc.channel.keepalive.timeout.sec * 1000,
                ),
                (
                    "grpc.keepalive_permit_without_calls",
                    int(
                        config.eggroll.core.grpc.channel.keepalive.permit.without.calls.enabled
                    ),
                ),
                (
                    "grpc.per_rpc_retry_buffer_size",
                    config.eggroll.core.grpc.channel.retry.buffer.size,
                ),
                ("grpc.enable_retries", 1),
                (
                    "grpc.service_config",
                    '{ "retryPolicy":{ '
                    '"maxAttempts": 4, "initialBackoff": "0.1s", '
                    '"maxBackoff": "1s", "backoffMutiplier": 2, '
                    '"retryableStatusCodes": [ "UNAVAILABLE" ] } }',
                ),
            ],
        )
