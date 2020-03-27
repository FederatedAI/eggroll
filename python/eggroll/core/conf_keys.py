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


class ErConfKey(object):
    def __init__(self, key, default_value=None):
        self.key = key
        self.default_value = default_value


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
    CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW = "eggroll.core.grpc.channel.flow.control.window"
    CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC = "eggroll.core.grpc.channel.idle.timeout.sec"
    CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC = "eggroll.core.grpc.channel.keepalive.time.sec"
    CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC = "eggroll.core.grpc.channel.keepalive.timout.sec"
    CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = "eggroll.core.grpc.channel.keepalive.without.calls.enabled"
    CONFKEY_CORE_GRPC_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC = "eggroll.core.grpc.channel.max.connection.age.grace.sec"
    CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE = "eggroll.core.grpc.channel.max.inbound.metadata.size"
    CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = "eggroll.core.grpc.channel.max.inbound.message.size"
    CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS = "eggroll.grpc.channel.max.retry.attempts"
    CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT = "eggroll.core.grpc.channel.per.rpc.buffer.limit"
    CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE = "eggroll.core.grpc.channel.retry.buffer.size"
    CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE = "eggroll.core.grpc.channel.ssl.session.cache.size"
    CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC = "eggroll.core.grpc.channel.ssl.session.timeout.sec"
    CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC = "eggroll.core.grpc.channel.termination.await.timeout.sec"

    CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW = "eggroll.core.grpc.server.channel.flow.control.window"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC = "eggroll.core.grpc.server.channel.keepalive.time.sec"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC = "eggroll.core.grpc.server.channel.keepalive.timeout.sec"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = "eggroll.core.grpc.server.channel.keepalive.without.calls.enabled"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION = "eggroll.core.grpc.server.channel.max.concurrent.call.per.connection"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC = "eggroll.core.grpc.server.channel.max.connection.age.sec"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC = "eggroll.core.grpc.server.channel.max.connection.idle.sec"
    CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = "eggroll.core.grpc.server.channel.max.inbound.message.size"
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


class TransferConfKeys(object):
    CONFKEY_TRANSFER_SERVICE_HOST = "eggroll.transfer.service.host"
    CONFKEY_TRANSFER_SERVICE_PORT = "eggroll.transfer.service.port"


class RollPairConfKeys(object):
    EGGROLL_ROLLPAIR_TRANSFERPAIR_SENDBUF_SIZE = ErConfKey("eggroll.rollpair.transferpair.sendbuf.size", 1 << 20)
    EGGROLL_ROLLPAIR_TRANSFERPAIR_BATCHBROKER_DEFAULT_SIZE = ErConfKey("eggroll.rollpair.transferpair.broker.default.size", 100)


class RollSiteConfKeys(object):
    EGGROLL_ROLLSITE_COORDINATOR = ErConfKey("eggroll.rollsite.coordinator")
    EGGROLL_ROLLSITE_HOST = ErConfKey("eggroll.rollsite.host", "127.0.0.1")
    EGGROLL_ROLLSITE_PORT = ErConfKey("eggroll.rollsite.port", "9370")
    EGGROLL_ROLLSITE_SECURE_PORT = ErConfKey("eggroll.rollsite.secure.port", "9380")
    EGGROLL_ROLLSITE_PARTY_ID = ErConfKey("eggroll.rollsite.party.id")
    EGGROLL_ROLLSITE_ROUTE_TABLE_PATH = ErConfKey("eggroll.rollsite.route.table.path", "conf/route_table.json")
    EGGROLL_ROLLSITE_PROXY_COMPATIBLE_ENABLED = ErConfKey("eggroll.rollsite.proxy.compatible.enabled", "false")
    EGGROLL_ROLLSITE_LAN_INSECURE_CHANNEL_ENABLED = ErConfKey("eggroll.rollsite.lan.insecure.channel.enabled")
    EGGROLL_ROLLSITE_AUDIT_ENABLED = ErConfKey("eggroll.rollsite.audit.enabled")
    EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE = ErConfKey("eggroll.rollsite.adapter.sendbuf.size", 32 << 20)
