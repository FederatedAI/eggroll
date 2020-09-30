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

import ipaddress
import threading

import grpc

from eggroll.core.conf_keys import CoreConfKeys
from eggroll.core.meta_model import ErEndpoint
from eggroll.utils.log_utils import get_logger

L = get_logger()

def wrap_host_scheme(host):
    try:
        ip = ipaddress.ip_address(host)
        return f'ipv{ip.version}:{host}'
    except ValueError as e:
        return host


class GrpcChannelFactory(object):
    pool = {}
    _pool_lock = threading.Lock()

    def create_channel(self, endpoint: ErEndpoint, is_secure_channel=False, refresh=False):
        target = f"{endpoint._host}:{endpoint._port}"
        with GrpcChannelFactory._pool_lock:
            result = GrpcChannelFactory.pool.get(target, None)
            result_status = grpc._common.CYGRPC_CONNECTIVITY_STATE_TO_CHANNEL_CONNECTIVITY[result._channel.check_connectivity_state(True)] if result is not None else None
            if result is None \
                    or refresh \
                    or result_status == grpc.ChannelConnectivity.SHUTDOWN:
                old_channel = result
                result = grpc.insecure_channel(
                target=target,
                options=[('grpc.max_send_message_length',
                          int(CoreConfKeys.EGGROLL_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.get())),
                         ('grpc.max_receive_message_length',
                          int(CoreConfKeys.EGGROLL_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.get())),
                         ('grpc.max_metadata_size',
                          int(CoreConfKeys.EGGROLL_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE.get())),
                         ('grpc.keepalive_time_ms', int(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC.get()) * 1000),
                         ('grpc.keepalive_timeout_ms', int(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC.get()) * 1000),
                         ('grpc.keepalive_permit_without_calls', int(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED.get())),
                         ('grpc.per_rpc_retry_buffer_size', int(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE.get())),
                         ('grpc.enable_retries', 1),
                         ('grpc.service_config',
                          '{ "retryPolicy":{ '
                          '"maxAttempts": 4, "initialBackoff": "0.1s", '
                          '"maxBackoff": "1s", "backoffMutiplier": 2, '
                          '"retryableStatusCodes": [ "UNAVAILABLE" ] } }')])
                GrpcChannelFactory.pool[target] = result
                # TODO:1: to decide if the old channel should be closed.
                if old_channel is not None:
                    L.debug(f"old channel to {target}'s status={result_status}")
                    #     old_channel.close()
            return GrpcChannelFactory.pool[target]

    @staticmethod
    def shutdown_all_now():
        for target, channel in GrpcChannelFactory.pool.items():
            L.debug(f"start to shutdown channel={channel}, target={target}")
            channel.close()
