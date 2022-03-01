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

from eggroll.core.utils import ErConfKey


class CoreConfKeys(object):
    LOGS_DIR = "eggroll.logs.dir"
    DATA_DIR = "eggroll.data.dir"
    STATIC_CONF_PATH = "eggroll.static.conf.path"
    BOOTSTRAP_ROOT_SCRIPT = "eggroll.bootstrap.root.script"
    BOOTSTRAP_SHELL = "eggroll.bootstrap.shell"
    BOOTSTRAP_SHELL_ARGS = "eggroll.bootstrap.shell.args"
    CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC = "eggroll.core.grpc.channel.cache.expire.sec"
    CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE = "eggroll.core.grpc.channel.cache.size"
    CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE = "eggroll.core.grpc.channel.executor.pool.size"
    CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW = ErConfKey("eggroll.core.grpc.channel.flow.control.window", 128 << 20)
    CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.channel.idle.timeout.sec", 86400)


    CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC = ErConfKey("eggroll.core.grpc.channel.keepalive.time.sec", 7200)
    CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.channel.keepalive.timout.sec", 3600)
    CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = ErConfKey("eggroll.core.grpc.channel.keepalive.without.calls.enabled", False)
    EGGROLL_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE = ErConfKey("eggroll.core.grpc.channel.max.inbound.metadata.size", 128 << 20)
    EGGROLL_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = ErConfKey("eggroll.core.grpc.channel.max.inbound.message.size", (2 << 30) - 1)
    CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS = ErConfKey("eggroll.grpc.channel.max.retry.attempts", 20)
    CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT = ErConfKey("eggroll.core.grpc.channel.per.rpc.buffer.limit", 64 << 20)
    CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE = ErConfKey("eggroll.core.grpc.channel.retry.buffer.size", 16 << 20)
    CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE = "eggroll.core.grpc.channel.ssl.session.cache.size"
    CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC = "eggroll.core.grpc.channel.ssl.session.timeout.sec"
    CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC = "eggroll.core.grpc.channel.termination.await.timeout.sec"

    CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW = "eggroll.core.grpc.server.channel.flow.control.window"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC = ErConfKey("eggroll.core.grpc.server.channel.keepalive.time.sec", 7200)
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.server.channel.keepalive.timeout.sec", 3600)
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = ErConfKey("eggroll.core.grpc.server.channel.keepalive.without.calls.enabled", False)
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION = "eggroll.core.grpc.server.channel.max.concurrent.call.per.connection"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC = "eggroll.core.grpc.server.channel.max.connection.age.sec"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC = "eggroll.core.grpc.server.channel.max.connection.idle.sec"
    EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = ErConfKey("eggroll.core.grpc.server.channel.max.inbound.message.size", (2 << 30) - 1)
    EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE = ErConfKey("eggroll.core.grpc.server.channel.max.inbound.metadata.size", 128 << 20)
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_RETRY_BUFFER_SIZE = ErConfKey("eggroll.core.grpc.server.channel.retry.buffer.size", 16 << 20)
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC = "eggroll.core.grpc.server.channel.permit.keepalive.time.sec"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE = "eggroll.core.grpc.server.channel.ssl.session.cache.size"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC = "eggroll.core.grpc.server.channel.ssl.session.timeout.sec"

    CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS = "eggroll.core.retry.default.attempt.timeout.ms"
    CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS = "eggroll.core.retry.default.max.attempts"
    CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS = "eggroll.core.retry.default.wait.time.ms"

    CONFKEY_CORE_SECURITY_CA_CRT_PATH = "eggroll.core.security.ca.crt.path"
    CONFKEY_CORE_SECURITY_KEY_CRT_PATH = "eggroll.core.security.crt.path"
    CONFKEY_CORE_SECURITY_KEY_PATH = "eggroll.core.security.key.path"
    CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED = "eggroll.core.security.secure.cluster.enabled"

    CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE = "eggroll.core.command.default.serdes.type"
    CONFKEY_CORE_LOG_DIR = "eggroll.core.log.dir"
    EGGROLL_CORE_FIFOBROKER_DEFAULT_SIZE = ErConfKey("eggroll.core.fifobroker.default.size", 16)
    EGGROLL_CORE_FIFOBROKER_ITER_TIMEOUT_SEC = ErConfKey("eggroll.core.fifobroker.iter.timeout.sec", 180)
    EGGROLL_CORE_CLIENT_COMMAND_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.core.client.command.executor.pool.max.size", 500)
    EGGROLL_CORE_DEFAULT_EXECUTOR_POOL = ErConfKey("eggroll.core.default.executor.pool", "eggroll.core.datastructure.threadpool.ErThreadUnpooledExecutor")
    EGGROLL_CORE_MALLOC_MMAP_THRESHOLD = ErConfKey("eggroll.core.malloc.mmap.threshold", 4_000)
    EGGROLL_CORE_MALLOC_MMAP_MAX = ErConfKey("eggroll.core.malloc.mmap.max", 200_000)


class ClusterManagerConfKeys(object):
    CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME = "eggroll.resourcemanager.clustermanager.jdbc.driver.class.name"
    CONFKEY_CLUSTER_MANAGER_JDBC_URL = "eggroll.resourcemanager.clustermanager.jdbc.url"
    CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME = "eggroll.resourcemanager.clustermanager.jdbc.username"
    CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD = "eggroll.resourcemanager.clustermanager.jdbc.password"
    CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE = "eggroll.resourcemanager.clustermanager.datasource.db.max.idle"
    CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL = "eggroll.resourcemanager.clustermanager.datasource.db.max.total"
    CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_WAIT_MS = "eggroll.resourcemanager.clustermanager.datasource.db.max.wait.ms"
    CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS = "eggroll.resourcemanager.clustermanager.datasource.db.time.between.eviction.runs.ms"
    CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS = "eggroll.resourcemanager.clustermanager.datasource.db.min.evictable.idle.time.ms"
    CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT = "eggroll.resourcemanager.clustermanager.datasource.db.default.auto.commit"
    CONFKEY_CLUSTER_MANAGER_HOST = "eggroll.resourcemanager.clustermanager.host"
    CONFKEY_CLUSTER_MANAGER_PORT = "eggroll.resourcemanager.clustermanager.port"


class NodeManagerConfKeys(object):
    CONFKEY_NODE_MANAGER_HOST = "eggroll.resourcemanager.nodemanager.host"
    CONFKEY_NODE_MANAGER_PORT = "eggroll.resourcemanager.nodemanager.port"


class SessionConfKeys(object):
    CONFKEY_SESSION_ID = "eggroll.session.id"
    CONFKEY_SESSION_NAME = "eggroll.session.name"
    CONFKEY_SESSION_PROCESSORS_PER_NODE = "eggroll.session.processors.per.node"
    CONFKEY_SESSION_DEPLOY_MODE = "eggroll.session.deploy.mode"
    CONFKEY_SESSION_STANDALONE_PORT = "eggroll.resourcemanager.standalone.port"
    EGGROLL_SESSION_START_TIMEOUT_MS = ErConfKey("eggroll.session.start.timeout.ms", 20_000)
    EGGROLL_SESSION_STOP_TIMEOUT_MS = ErConfKey("eggroll.session.stop.timeout.ms", 20_000)

class TransferConfKeys(object):
    CONFKEY_TRANSFER_SERVICE_HOST = "eggroll.transfer.service.host"
    CONFKEY_TRANSFER_SERVICE_PORT = "eggroll.transfer.service.port"


class RollPairConfKeys(object):
    EGGROLL_ROLLPAIR_TRANSFERPAIR_SENDBUF_SIZE = ErConfKey("eggroll.rollpair.transferpair.sendbuf.size", 250_000)
    EGGROLL_ROLLPAIR_TRANSFERPAIR_BATCHBROKER_DEFAULT_SIZE = ErConfKey("eggroll.rollpair.transferpair.broker.default.size", 100)
    EGGROLL_ROLLPAIR_EGGPAIR_SERVER_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.rollpair.eggpair.server.executor.pool.max.size", 5_000)
    EGGROLL_ROLLPAIR_EGGPAIR_DATA_SERVER_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.rollpair.eggpair.data.server.executor.pool.max.size", 5_000)
    EGGROLL_ROLLPAIR_TRANSFERPAIR_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.rollpair.transferpair.executor.pool.max.size", 5_000)
    EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE = ErConfKey("eggroll.rollpair.default.store.type", "ROLLPAIR_LMDB")
    EGGROLL_ROLLPAIR_ROCKSDB_WRITEBATCH_SIZE = ErConfKey("eggroll.rollpair.rocksdb.writebatch.size", 500)
    EGGROLL_ROLLPAIR_REMOTE_WRITEBATCH_SIZE = ErConfKey("eggroll.rollpair.remote.writebatch.size", 1_000_000)
    EGGROLL_ROLLPAIR_STORAGE_REPLICA_COUNT = ErConfKey("eggroll.rollpair.storage.replica.count", 1)
    EGGROLL_ROLLPAIR_STORAGE_REPLICATE_ENABLED = ErConfKey("eggroll.rollpair.storage.replicate.enabled", False)
    EGGROLL_ROLLPAIR_STORAGE_REPLICATE_TEMP_FILES = ErConfKey("eggroll.rollpair.storage.replicate.temp.files", False)
    EGGROLL_ROLLPAIR_IN_MEMORY_OUTPUT = ErConfKey("eggroll.rollpair.inmemory_output", False)


class RollSiteConfKeys(object):
    EGGROLL_ROLLSITE_PULL_MAX_RETRY = ErConfKey("eggroll.rollsite.pull.max.retry", 720)
    EGGROLL_ROLLSITE_PULL_INTERVAL_SEC = ErConfKey("eggroll.rollsite.pull.interval.sec", 600)
    EGGROLL_ROLLSITE_PULL_HEADER_INTERVAL_SEC = ErConfKey("eggroll.rollsite.pull.header.interval.sec", 300)
    EGGROLL_ROLLSITE_PULL_HEADER_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.pull.header.timeout.sec", 720 * 600)
    EGGROLL_ROLLSITE_PUSH_BATCHES_PER_STREAM = ErConfKey("eggroll.rollsite.push.batches.per.stream", 10)
    EGGROLL_ROLLSITE_PUSH_MAX_RETRY = ErConfKey("eggroll.rollsite.push.max.retry", 3)
    EGGROLL_ROLLSITE_PUSH_LONG_RETRY = ErConfKey("eggroll.rollsite.push.long.retry", 2)
    EGGROLL_ROLLSITE_PUSH_OVERALL_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.push.overall.timeout.sec", 600)
    EGGROLL_ROLLSITE_PUSH_PER_STREAM_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.push.per.stream.timeout.sec", 300)

    EGGROLL_ROLLSITE_COORDINATOR = ErConfKey("eggroll.rollsite.coordinator")
    EGGROLL_ROLLSITE_DEPLOY_MODE = ErConfKey("eggroll.rollsite.deploy.mode", "cluster")
    EGGROLL_ROLLSITE_HOST = ErConfKey("eggroll.rollsite.host", "127.0.0.1")
    EGGROLL_ROLLSITE_PORT = ErConfKey("eggroll.rollsite.port", "9370")
    EGGROLL_ROLLSITE_SECURE_PORT = ErConfKey("eggroll.rollsite.secure.port", "9380")
    EGGROLL_ROLLSITE_PARTY_ID = ErConfKey("eggroll.rollsite.party.id")
    EGGROLL_ROLLSITE_ROUTE_TABLE_PATH = ErConfKey("eggroll.rollsite.route.table.path", "conf/route_table.json")
    EGGROLL_ROLLSITE_PROXY_COMPATIBLE_ENABLED = ErConfKey("eggroll.rollsite.proxy.compatible.enabled", "false")
    EGGROLL_ROLLSITE_LAN_INSECURE_CHANNEL_ENABLED = ErConfKey("eggroll.rollsite.lan.insecure.channel.enabled")
    EGGROLL_ROLLSITE_AUDIT_ENABLED = ErConfKey("eggroll.rollsite.audit.enabled")
    EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE = ErConfKey("eggroll.rollsite.adapter.sendbuf.size", 100_000)
    EGGROLL_ROLLSITE_RECEIVE_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.rollsite.receive.executor.pool.max.size", 5_000)
    EGGROLL_ROLLSITE_COMPLETE_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.rollsite.complete.executor.pool.max.size", 50)
    EGGROLL_ROLLSITE_UNARYCALL_CLIENT_MAX_RETRY = ErConfKey("eggroll.rollsite.unarycall.client.max.retry", 100)
    EGGROLL_ROLLSITE_UNARYCALL_MAX_RETRY = ErConfKey("eggroll.rollsite.unarycall.max.retry", 30_000)
    EGGROLL_ROLLSITE_PUSH_CLIENT_MAX_RETRY = ErConfKey("eggroll.rollsite.push.client.max.retry", 10)
    EGGROLL_ROLLSITE_PULL_CLIENT_MAX_RETRY = ErConfKey("eggroll.rollsite.pull.client.max.retry", 300)
    EGGROLL_ROLLSITE_OVERALL_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.overall.timeout.sec", 172_800_000)
    EGGROLL_ROLLSITE_COMPLETION_WAIT_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.completion.wait.timeout.sec", 3_600_000)
    EGGROLL_ROLLSITE_PACKET_INTERVAL_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.packet.interval.timeout.sec", 20_000)

    EGGROLL_ROLLSITE_PUSH_SESSION_ENABLED =  ErConfKey("eggroll.rollsite.push.session.enabled", False)
