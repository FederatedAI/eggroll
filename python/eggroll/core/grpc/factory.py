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

import grpc

from eggroll.core.meta_model import ErEndpoint


def wrap_host_scheme(host):
    try:
        ip = ipaddress.ip_address(host)
        return f'ipv{ip.version}:{host}'
    except ValueError as e:
        return host


class GrpcChannelFactory(object):
    pool = {}

    def create_channel(self, endpoint: ErEndpoint, is_secure_channel=False,
                       root_certificates=None, private_key=None, certificate_chain=None):
        target = f"{endpoint._host}:{endpoint._port}"
        if target not in self.pool:
            if is_secure_channel is True:
                creds = grpc.ssl_channel_credentials(root_certificates, private_key, certificate_chain)
                result = grpc.secure_channel(
                    target=target,
                    credentials=creds,
                    options=[('grpc.max_send_message_length', 2 << 30 - 1),
                             ('grpc.max_receive_message_length', 2 << 30 - 1),
                             ('grpc.max_metadata_size', 32 << 20)])
            else:
                result = grpc.insecure_channel(
                    target=target,
                    options=[('grpc.max_send_message_length', 2 << 30 - 1),
                             ('grpc.max_receive_message_length', 2 << 30 - 1),
                             ('grpc.max_metadata_size', 32 << 20)])
            self.pool[target] = result
        return self.pool[target]
