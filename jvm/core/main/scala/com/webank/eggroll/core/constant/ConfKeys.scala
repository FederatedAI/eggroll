/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.constant

object CoreConfKeys {
  val CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC = "eggroll.core.grpc.channel.cache.expire.sec"
  val CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE = "eggroll.core.grpc.channel.cache.size"
  val CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE = "eggroll.core.grpc.channel.executor.pool.size"
  val CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW = "eggroll.core.grpc.channel.flow.control.window"
  val CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC = "eggroll.core.grpc.channel.idle.timeout.sec"
  val CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC = "eggroll.core.grpc.channel.keepalive.time.sec"
  val CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC = "eggroll.core.grpc.channel.keepalive.timout.sec"
  val CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = "eggroll.core.grpc.channel.keepalive.without.calls.enabled"
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC = "eggroll.core.grpc.channel.max.connection.age.grace.sec"
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE = "eggroll.core.grpc.channel.max.inbound.metadata.size"
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = "eggroll.core.grpc.channel.max.inbound.message.size"
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS = "eggroll.grpc.channel.max.retry.attempts"
  val CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT = "eggroll.core.grpc.channel.per.rpc.buffer.limit"
  val CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE = "eggroll.core.grpc.channel.retry.buffer.size"
  val CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE = "eggroll.core.grpc.channel.ssl.session.cache.size"
  val CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC = "eggroll.core.grpc.channel.ssl.session.timeout.sec"
  val CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC = "eggroll.core.grpc.channel.termination.await.timeout.sec"

  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW = "eggroll.core.grpc.server.channel.flow.control.window"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC = "eggroll.core.grpc.server.channel.keepalive.time.sec"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC = "eggroll.core.grpc.server.channel.keepalive.timeout.sec"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = "eggroll.core.grpc.server.channel.keepalive.without.calls.enabled"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION = "eggroll.core.grpc.server.channel.max.concurrent.call.per.connection"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC = "eggroll.core.grpc.server.channel.max.connection.age.sec"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC = "eggroll.core.grpc.server.channel.max.connection.idle.sec"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = "eggroll.core.grpc.server.channel.max.inbound.message.size"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC = "eggroll.core.grpc.server.channel.permit.keepalive.time.sec"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE = "eggroll.core.grpc.server.channel.ssl.session.cache.size"
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC = "eggroll.core.grpc.server.channel.ssl.session.timeout.sec"

  val CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS = "eggroll.core.retry.default.attempt.timeout.ms"
  val CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS = "eggroll.core.retry.default.max.attempts"
  val CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS = "eggroll.core.retry.default.wait.time.ms"

  val CONFKEY_CORE_SECURITY_CA_CRT_PATH = "eggroll.core.security.ca.crt.path"
  val CONFKEY_CORE_SECURITY_KEY_CRT_PATH = "eggroll.core.security.crt.path"
  val CONFKEY_CORE_SECURITY_KEY_PATH = "eggroll.core.security.key.path"
  val CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED = "eggroll.core.security.secure.cluster.enabled"

  val CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE = "eggroll.core.command.default.serdes.type"
  val CONFKEY_CORE_LOG_DIR = "eggroll.core.log.dir"
}

object ClusterManagerConfKeys {
  val CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME = "eggroll.cluster.manager.jdbc.driver.class.name"
  val CONFKEY_CLUSTER_MANAGER_JDBC_URL = "eggroll.cluster.manager.jdbc.url"
  val CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME = "eggroll.cluster.manager.jdbc.username"
  val CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD = "eggroll.cluster.manager.jdbc.password"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE = "eggroll.cluster.manager.datasource.db.max.idle"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL = "eggroll.cluster.manager.datasource.db.max.total"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_WAIT_MS = "eggroll.cluster.manager.datasource.db.max.wait.ms"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS = "eggroll.cluster.manager.datasource.db.time.between.eviction.runs.ms"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS = "eggroll.cluster.manager.datasource.db.min.evictable.idle.time.ms"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT = "eggroll.cluster.manager.datasource.db.default.auto.commit"
  val CONFKEY_CLUSTER_MANAGER_HOST = "eggroll.cluster.manager.host"
  val CONFKEY_CLUSTER_MANAGER_PORT = "eggroll.cluster.manager.port"
}

object NodeManagerConfKeys {
  val CONFKEY_NODE_MANAGER_HOST = "eggroll.node.manager.host"
  val CONFKEY_NODE_MANAGER_PORT = "eggroll.node.manager.port"
}

object SessionConfKeys {
  val CONFKEY_SESSION_CONTEXT_ROLLPAIR_COUNT = "eggroll.session.context.rollpair.count"
  val CONFKEY_SESSION_ID = "eggroll.session.id"
  val CONFKEY_SESSION_NAME = "eggroll.session.name"
  val CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE = "eggroll.session.max.processors.per.node"
}

object DeployConfKeys {
  val CONFKEY_DEPLOY_ROLLPAIR_START_SCRIPT_PATH = "eggroll.deploy.rollpair.start.script.path"
  val CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH = "eggroll.deploy.rollpair.venv.path"
  val CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH = "eggroll.deploy.rollpair.data.dir.path"
  val CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH = "eggroll.deploy.rollpair.eggpair.path"
  val CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH = "eggroll.deploy.rollpair.python.path"
  val CONFKEY_DEPLOY_NODE_MANAGER_PORT = "eggroll.deploy.node.manager.port"

  val CONFKEY_DEPLOY_JVM_JAVA_BIN_PATH = "eggroll.deploy.jvm.java.bin.path"
  val CONFKEY_DEPLOY_JVM_CLASSPATH = "eggroll.deploy.jvm.classpath"
  val CONFKEY_DEPLOY_JVM_MAINCLASS = "eggroll.deploy.jvm.mainclass"
  val CONFKEY_DEPLOY_JVM_MAINCLASS_ARGS = "eggroll.deploy.jvm.mainclass.args"
  val CONFKEY_DEPLOY_JVM_OPTIONS = "eggroll.deploy.jvm.options"

  val CONFKEY_DEPLOY_PROCESSOR_TYPE = "eggroll.deploy.processor.type"
}