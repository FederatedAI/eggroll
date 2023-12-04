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

# from eggroll.core.utils import ErConfKey
#
#
# class RollSiteConfKeys(object):
#     EGGROLL_ROLLSITE_PULL_MAX_RETRY = ErConfKey("eggroll.rollsite.pull.max.retry", 720)
#     EGGROLL_ROLLSITE_PULL_INTERVAL_SEC = ErConfKey(
#         "eggroll.rollsite.pull.interval.sec", 600
#     )
#     EGGROLL_ROLLSITE_PULL_HEADER_INTERVAL_SEC = ErConfKey(
#         "eggroll.rollsite.pull.header.interval.sec", 300
#     )
#     EGGROLL_ROLLSITE_PULL_HEADER_TIMEOUT_SEC = ErConfKey(
#         "eggroll.rollsite.pull.header.timeout.sec", 720 * 600
#     )
#     EGGROLL_ROLLSITE_PUSH_BATCHES_PER_STREAM = ErConfKey(
#         "eggroll.rollsite.push.batches.per.stream", 10
#     )
#     EGGROLL_ROLLSITE_PUSH_MAX_RETRY = ErConfKey("eggroll.rollsite.push.max.retry", 3)
#     EGGROLL_ROLLSITE_PUSH_LONG_RETRY = ErConfKey("eggroll.rollsite.push.long.retry", 2)
#     EGGROLL_ROLLSITE_PUSH_OVERALL_TIMEOUT_SEC = ErConfKey(
#         "eggroll.rollsite.push.overall.timeout.sec", 600
#     )
#     EGGROLL_ROLLSITE_PUSH_PER_STREAM_TIMEOUT_SEC = ErConfKey(
#         "eggroll.rollsite.push.per.stream.timeout.sec", 300
#     )
#
#     EGGROLL_ROLLSITE_COORDINATOR = ErConfKey("eggroll.rollsite.coordinator")
#     EGGROLL_ROLLSITE_DEPLOY_MODE = ErConfKey("eggroll.rollsite.deploy.mode", "cluster")
#     EGGROLL_ROLLSITE_HOST = ErConfKey("eggroll.rollsite.host", "127.0.0.1")
#     EGGROLL_ROLLSITE_PORT = ErConfKey("eggroll.rollsite.port", "9370")
#     EGGROLL_ROLLSITE_SECURE_PORT = ErConfKey("eggroll.rollsite.secure.port", "9380")
#     EGGROLL_ROLLSITE_PARTY_ID = ErConfKey("eggroll.rollsite.party.id")
#     EGGROLL_ROLLSITE_ROUTE_TABLE_PATH = ErConfKey(
#         "eggroll.rollsite.route.table.path", "conf/route_table.json"
#     )
#     EGGROLL_ROLLSITE_PROXY_COMPATIBLE_ENABLED = ErConfKey(
#         "eggroll.rollsite.proxy.compatible.enabled", "false"
#     )
#     EGGROLL_ROLLSITE_LAN_INSECURE_CHANNEL_ENABLED = ErConfKey(
#         "eggroll.rollsite.lan.insecure.channel.enabled"
#     )
#     EGGROLL_ROLLSITE_AUDIT_ENABLED = ErConfKey("eggroll.rollsite.audit.enabled")
#     EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE = ErConfKey(
#         "eggroll.rollsite.adapter.sendbuf.size", 100_000
#     )
#     EGGROLL_ROLLSITE_RECEIVE_EXECUTOR_POOL_MAX_SIZE = ErConfKey(
#         "eggroll.rollsite.receive.executor.pool.max.size", 5_000
#     )
#     EGGROLL_ROLLSITE_COMPLETE_EXECUTOR_POOL_MAX_SIZE = ErConfKey(
#         "eggroll.rollsite.complete.executor.pool.max.size", 50
#     )
#     EGGROLL_ROLLSITE_UNARYCALL_CLIENT_MAX_RETRY = ErConfKey(
#         "eggroll.rollsite.unarycall.client.max.retry", 100
#     )
#     EGGROLL_ROLLSITE_UNARYCALL_MAX_RETRY = ErConfKey(
#         "eggroll.rollsite.unarycall.max.retry", 30_000
#     )
#     EGGROLL_ROLLSITE_PUSH_CLIENT_MAX_RETRY = ErConfKey(
#         "eggroll.rollsite.push.client.max.retry", 10
#     )
#     EGGROLL_ROLLSITE_PULL_CLIENT_MAX_RETRY = ErConfKey(
#         "eggroll.rollsite.pull.client.max.retry", 300
#     )
#     EGGROLL_ROLLSITE_OVERALL_TIMEOUT_SEC = ErConfKey(
#         "eggroll.rollsite.overall.timeout.sec", 172_800_000
#     )
#     EGGROLL_ROLLSITE_COMPLETION_WAIT_TIMEOUT_SEC = ErConfKey(
#         "eggroll.rollsite.completion.wait.timeout.sec", 3_600_000
#     )
#     EGGROLL_ROLLSITE_PACKET_INTERVAL_TIMEOUT_SEC = ErConfKey(
#         "eggroll.rollsite.packet.interval.timeout.sec", 20_000
#     )
#
#     EGGROLL_ROLLSITE_PUSH_SESSION_ENABLED = ErConfKey(
#         "eggroll.rollsite.push.session.enabled", False
#     )
