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



class CoreConfKeys(object):
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


class ClusterManagerConfKeys(object):
  CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME = "eggroll.cluster.manager.jdbc.driver.class.name"
  CONFKEY_CLUSTER_MANAGER_JDBC_URL = "eggroll.cluster.manager.jdbc.url"
  CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME = "eggroll.cluster.manager.jdbc.username"
  CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD = "eggroll.cluster.manager.jdbc.password"
  CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE = "eggroll.cluster.manager.datasource.db.max.idle"
  CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL = "eggroll.cluster.manager.datasource.db.max.total"
  CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_WAIT_MS = "eggroll.cluster.manager.datasource.db.max.wait.ms"
  CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS = "eggroll.cluster.manager.datasource.db.time.between.eviction.runs.ms"
  CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS = "eggroll.cluster.manager.datasource.db.min.evictable.idle.time.ms"
  CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT = "eggroll.cluster.manager.datasource.db.default.auto.commit"
  CONFKEY_CLUSTER_MANAGER_HOST = "eggroll.cluster.manager.host"
  CONFKEY_CLUSTER_MANAGER_PORT = "eggroll.cluster.manager.port"


class NodeManagerConfKeys(object):
  CONFKEY_NODE_MANAGER_HOST = "eggroll.node.manager.host"
  CONFKEY_NODE_MANAGER_PORT = "eggroll.node.manager.port"


class SessionConfKeys(object):
  CONFKEY_SESSION_CONTEXT_ROLLPAIR_COUNT = "eggroll.session.context.rollpair.count"
  CONFKEY_SESSION_ID = "eggroll.session.id"
  CONFKEY_SESSION_NAME = "eggroll.session.name"
  CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE = "eggroll.session.max.processors.per.node"


class DeployConfKeys(object):
  CONFKEY_DEPLOY_ROLLPAIR_START_SCRIPT_PATH = "eggroll.deploy.rollpair.start.script.path"
  CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH = "eggroll.deploy.rollpair.venv.path"
  CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH = "eggroll.deploy.rollpair.data.dir.path"
  CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH = "eggroll.deploy.rollpair.eggpair.path"
  CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH = "eggroll.deploy.rollpair.python.path"
  CONFKEY_DEPLOY_NODE_MANAGER_PORT = "eggroll.deploy.node.manager.port"

  CONFKEY_DEPLOY_JVM_JAVA_BIN_PATH = "eggroll.deploy.jvm.java.bin.path"
  CONFKEY_DEPLOY_JVM_CLASSPATH = "eggroll.deploy.jvm.classpath"
  CONFKEY_DEPLOY_JVM_MAINCLASS = "eggroll.deploy.jvm.mainclass"
  CONFKEY_DEPLOY_JVM_MAINCLASS_ARGS = "eggroll.deploy.jvm.mainclass.args"
  CONFKEY_DEPLOY_JVM_OPTIONS = "eggroll.deploy.jvm.options"

  CONFKEY_DEPLOY_PROCESSOR_TYPE = "eggroll.deploy.processor.type"
