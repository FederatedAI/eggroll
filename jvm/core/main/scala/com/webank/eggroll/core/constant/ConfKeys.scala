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

import com.webank.eggroll.core.session.StaticErConf

case class ErConfKey(key: String, defaultValue: String = StringConstants.EMPTY) {

  def get(): String = {
    getWith()
  }

  def getWith(candidates: List[Map[String, String]] = List()): String = {
    var result: String = null

    for (c <- candidates) {
      if (c.contains(key)) result = c(key)
    }

    if (result == null) result = StaticErConf.getString(key, defaultValue)

    result
  }

  def getWith(candidate: Map[String, String]): String = {
    getWith(List(candidate))
  }
}

object ErConfKey {
  def apply(key: String, defaultValue: Any): ErConfKey = {
    if (defaultValue != null) ErConfKey(key, defaultValue.toString)
    else ErConfKey(key)
  }
}


object CoreConfKeys {
  val EGGROLL_LOGS_DIR = ErConfKey("eggroll.logs.dir")
  val EGGROLL_DATA_DIR = ErConfKey("eggroll.data.dir")
  val STATIC_CONF_PATH = "eggroll.static.conf.path"
  val BOOTSTRAP_ROOT_SCRIPT = "eggroll.bootstrap.root.script"
  val BOOTSTRAP_SHELL = "eggroll.bootstrap.shell"
  val BOOTSTRAP_SHELL_ARGS = "eggroll.bootstrap.shell.args"
  val CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC = ErConfKey("eggroll.core.grpc.channel.cache.expire.sec", 86400)
  val CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE = ErConfKey("eggroll.core.grpc.channel.cache.size", 5000)
  val CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE = ErConfKey("eggroll.core.grpc.channel.executor.pool.size", 128)
  val CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW = ErConfKey("eggroll.core.grpc.channel.flow.control.window", 128 << 20)
  val CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.channel.idle.timeout.sec", 86400)
  val CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC = ErConfKey("eggroll.core.grpc.channel.keepalive.time.sec", 7200)
  val CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.channel.keepalive.timeout.sec", 3600)
  val CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = ErConfKey("eggroll.core.grpc.channel.keepalive.without.calls.enabled", false)
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE = ErConfKey("eggroll.core.grpc.channel.max.inbound.metadata.size", 128 << 20)
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = ErConfKey("eggroll.core.grpc.channel.max.inbound.message.size", (2 << 30) - 1)
  val CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS = ErConfKey("eggroll.grpc.channel.max.retry.attempts", 5)
  val CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT = ErConfKey("eggroll.core.grpc.channel.per.rpc.buffer.limit", 64 << 20)
  val CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE = ErConfKey("eggroll.core.grpc.channel.retry.buffer.size", 16 << 20)
  val CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE = ErConfKey("eggroll.core.grpc.channel.ssl.session.cache.size", 65536)
  val CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.channel.ssl.session.timeout.sec", 7200)
  val CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.channel.termination.await.timeout.sec", 20)

  val CONFKEY_CORE_GRPC_TRANSFER_SERVER_HOST = ErConfKey("eggroll.core.grpc.transfer.server.host", "0.0.0.0")
  val CONFKEY_CORE_GRPC_TRANSFER_SERVER_PORT = ErConfKey("eggroll.core.grpc.transfer.server.port", "0")
  val CONFKEY_CORE_GRPC_TRANSFER_SECURE_SERVER_ENABLED = "eggroll.core.grpc.transfer.secure.server.enabled"


  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW = ErConfKey("eggroll.core.grpc.server.channel.flow.control.window", 128 << 20)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC = ErConfKey("eggroll.core.grpc.server.channel.keepalive.time.sec", 7200)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.server.channel.keepalive.timeout.sec", 3600)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED = ErConfKey("eggroll.core.grpc.server.channel.keepalive.without.calls.enabled", false)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION = ErConfKey("eggroll.core.grpc.server.channel.max.concurrent.call.per.connection", 1000)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC = ErConfKey("eggroll.core.grpc.server.channel.max.connection.age.sec", 86400)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC = ErConfKey("eggroll.core.grpc.server.channel.max.connection.age.grace.sec", 86400)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC = ErConfKey("eggroll.core.grpc.server.channel.max.connection.idle.sec", 86400)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE = ErConfKey("eggroll.core.grpc.server.channel.max.inbound.message.size", (2 << 30) - 1)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE = ErConfKey("eggroll.core.grpc.server.channel.max.inbound.metadata.size", 128 << 20)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC = ErConfKey("eggroll.core.grpc.server.channel.permit.keepalive.time.sec", 120)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE = ErConfKey("eggroll.core.grpc.server.channel.ssl.session.cache.size", 65536)
  val CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC = ErConfKey("eggroll.core.grpc.server.channel.ssl.session.timeout.sec", 86400)

  val CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS = ErConfKey("eggroll.core.retry.default.attempt.timeout.ms", 30000)
  val CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS = ErConfKey("eggroll.core.retry.default.max.attempts", 3)
  val CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS = ErConfKey("eggroll.core.retry.default.wait.time.ms", 1000)

  val CONFKEY_CORE_SECURITY_CA_CRT_PATH = ErConfKey("eggroll.core.security.ca.crt.path")
  val CONFKEY_CORE_SECURITY_KEY_CRT_PATH = ErConfKey("eggroll.core.security.crt.path")
  val CONFKEY_CORE_SECURITY_KEY_PATH = ErConfKey("eggroll.core.security.key.path")
  val CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH = ErConfKey("eggroll.core.security.client.ca.crt.path")
  val CONFKEY_CORE_SECURITY_CLIENT_KEY_CRT_PATH = ErConfKey("eggroll.core.security.client.crt.path")
  val CONFKEY_CORE_SECURITY_CLIENT_KEY_PATH = ErConfKey("eggroll.core.security.client.key.path")
  val CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED = ErConfKey("eggroll.core.security.secure.cluster.enabled", false)
  val CONFKEY_CORE_SECURITY_CLIENT_AUTH_ENABLED = ErConfKey("eggroll.core.security.secure.client.auth.enabled", false)

  val CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE = "eggroll.core.command.default.serdes.type"
  val CONFKEY_CORE_LOG_DIR = "eggroll.core.log.dir"

  val EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS = ErConfKey("eggroll.core.stats.direct.memory.metrics", false)
  val EGGROLL_CORE_STATS_DIRECT_MEMORY_METRICS_INTERVAL_SEC = ErConfKey("eggroll.core.stats.direct.memory.metrics.interval.sec", 60)
}

object ClusterManagerConfKeys {
  val CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME = "eggroll.resourcemanager.clustermanager.jdbc.driver.class.name"
  val CONFKEY_CLUSTER_MANAGER_JDBC_URL = "eggroll.resourcemanager.clustermanager.jdbc.url"
  val CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME = "eggroll.resourcemanager.clustermanager.jdbc.username"
  val CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD = "eggroll.resourcemanager.clustermanager.jdbc.password"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE = "eggroll.resourcemanager.clustermanager.datasource.db.max.idle"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL = "eggroll.resourcemanager.clustermanager.datasource.db.max.total"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_WAIT_MS = "eggroll.resourcemanager.clustermanager.datasource.db.max.wait.ms"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS = "eggroll.resourcemanager.clustermanager.datasource.db.time.between.eviction.runs.ms"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS = "eggroll.resourcemanager.clustermanager.datasource.db.min.evictable.idle.time.ms"
  val CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT = "eggroll.resourcemanager.clustermanager.datasource.db.default.auto.commit"
  val CONFKEY_CLUSTER_MANAGER_HOST = "eggroll.resourcemanager.clustermanager.host"
  val CONFKEY_CLUSTER_MANAGER_PORT = "eggroll.resourcemanager.clustermanager.port"
  val EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR = ErConfKey("eggroll.resourcemanager.clustermanager.jdbc.password.decryptor")
  val EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR_ARGS = ErConfKey("eggroll.resourcemanager.clustermanager.jdbc.password.decryptor.args")
  val EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR_ARGS_SPLITER = ErConfKey("eggroll.resourcemanager.clustermanager.jdbc.password.decryptor.args.spliter", ",")
}

object NodeManagerConfKeys {
  val CONFKEY_NODE_MANAGER_HOST = "eggroll.resourcemanager.nodemanager.host"
  val CONFKEY_NODE_MANAGER_PORT = "eggroll.resourcemanager.nodemanager.port"
}

object ResourceManagerConfKeys {
  val SERVER_NODE_ID = "eggroll.resourcemanager.server.node.id"
}

object SessionConfKeys {
  val CONFKEY_SESSION_CONTEXT_ROLLPAIR_COUNT = "eggroll.session.context.rollpair.count"
  val CONFKEY_SESSION_ID = "eggroll.session.id"
  val CONFKEY_SESSION_NAME = "eggroll.session.name"
  val CONFKEY_SESSION_PROCESSORS_PER_NODE = "eggroll.session.processors.per.node"
  val EGGROLL_SESSION_START_TIMEOUT_MS = ErConfKey("eggroll.session.start.timeout.ms", 20000)
  val EGGROLL_SESSION_STOP_TIMEOUT_MS = ErConfKey("eggroll.session.stop.timeout.ms", 20000)
  val EGGROLL_SESSION_PYTHON_PATH = "python.path"
  val EGGROLL_SESSION_PYTHON_VENV = "python.venv"
}

object DeployConfKeys {
  val CONFKEY_DEPLOY_MODE = "eggroll.deploy.mode"
}

object RollSiteConfKeys {
  val EGGROLL_ROLLSITE_POLLING_Q_POLL_INTERVAL_SEC = ErConfKey("eggroll.rollsite.polling.q.poll.interval.sec", 60)
  val EGGROLL_ROLLSITE_POLLING_Q_OFFER_INTERVAL_SEC = ErConfKey("eggroll.rollsite.polling.q.offer.interval.sec", 60)
  val EGGROLL_ROLLSITE_POLLING_NO_DATA_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.polling.no.data.timeout.sec", 300)
  val EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.polling.exchanger.data.op.timeout.sec", 240) // should always be < no data timeout and python per stream deadline
  val EGGROLL_ROLLSITE_COORDINATOR = ErConfKey("eggroll.rollsite.coordinator")
  val EGGROLL_ROLLSITE_HOST = ErConfKey("eggroll.rollsite.host", "127.0.0.1")
  val EGGROLL_ROLLSITE_PORT = ErConfKey("eggroll.rollsite.port", "9370")
  val EGGROLL_ROLLSITE_SECURE_PORT = ErConfKey("eggroll.rollsite.secure.port", "9380")
  val EGGROLL_ROLLSITE_PARTY_ID = ErConfKey("eggroll.rollsite.party.id")
  val EGGROLL_ROLLSITE_ROUTE_TABLE_PATH = ErConfKey("eggroll.rollsite.route.table.path", "conf/route_table.json")
  val EGGROLL_ROLLSITE_PROXY_COMPATIBLE_ENABLED = ErConfKey("eggroll.rollsite.proxy.compatible.enabled", "false")
  val EGGROLL_ROLLSITE_LAN_INSECURE_CHANNEL_ENABLED = ErConfKey("eggroll.rollsite.lan.insecure.channel.enabled")
  val EGGROLL_ROLLSITE_AUDIT_ENABLED = ErConfKey("eggroll.rollsite.audit.enabled")
  val EGGROLL_ROLLSITE_UNARYCALL_CLIENT_MAX_RETRY = ErConfKey("eggroll.rollsite.unarycall.client.max.retry", 100)
  val EGGROLL_ROLLSITE_UNARYCALL_MAX_RETRY = ErConfKey("eggroll.rollsite.unarycall.client.max.retry", 30000)
  val EGGROLL_ROLLSITE_PUSH_CLIENT_MAX_RETRY = ErConfKey("eggroll.rollsite.push.client.max.retry", 10)
  val EGGROLL_ROLLSITE_PUSH_MAX_RETRY = ErConfKey("eggroll.rollsite.push.max.retry", 300)
  val EGGROLL_ROLLSITE_PULL_CLIENT_MAX_RETRY = ErConfKey("eggroll.rollsite.pull.client.max.retry", 300)
  val EGGROLL_ROLLSITE_OVERALL_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.overall.timeout.sec", 172800000)
  val EGGROLL_ROLLSITE_ONCOMPLETED_WAIT_TIMEOUT = ErConfKey("eggroll.rollsite.oncompleted.wait.timeout.sec", 600)
  val EGGROLL_ROLLSITE_PACKET_INTERVAL_TIMEOUT = ErConfKey("eggroll.rollsite.packet.interval.timeout.sec", 20000)
  val EGGROLL_ROLLSITE_PULL_OBJECT_TIMEOUT_SEC = ErConfKey("eggroll.rollsite.pull.object.timeout.sec", 1800)
  val EGGROLL_ROLLSITE_ROUTE_TABLE_WHITELIST = ErConfKey("eggroll.rollsite.route.table.whitelist")
  val EGGROLL_ROLLSITE_AUDIT_TOPICS = ErConfKey("eggroll.rollsite.audit.topics")
  val EGGROLL_ROLLSITE_POLLING_SERVER_ENABLED = ErConfKey("eggroll.rollsite.polling.server.enabled", false)
  val EGGROLL_ROLLSITE_POLLING_CLIENT_ENABLED = ErConfKey("eggroll.rollsite.polling.client.enabled", false)
  val EGGROLL_ROLLSITE_POLLING_CONCURRENCY = ErConfKey("eggroll.rollsite.polling.concurrency", 50)
  val EGGROLL_ROLLSITE_ROUTE_TABLE_KEY = ErConfKey("eggroll.rollsite.route.table.key", "123")
  val EGGROLL_ROLLSITE_POLLING_AHTHENTICATION_ENABLED = ErConfKey("eggroll.rollsite.polling.authentication.enabled", false)
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_USE_CONFIG = ErConfKey("eggroll.rollsite.polling.authentication.use.config", false)
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_URI = ErConfKey("eggroll.rollsite.polling.authentication.uri")
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_APPKEY = ErConfKey("eggroll.rollsite.polling.authentication.appkey")
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_APPSERCRET = ErConfKey("eggroll.rollsite.polling.authentication.appsecret")
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_ROLE = ErConfKey("eggroll.rollsite.polling.authentication.role", "Guest")
  val EGGROLL_ROLLSITE_POLLING_SECRET_INFO_URL = ErConfKey("eggroll.rollsite.polling.secret.info.url")
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_URL = ErConfKey("eggroll.rollsite.polling.authentication.url")
  val EGGROLL_ROLLSITE_POLLING_AUTHENTICATOR_CLASS = ErConfKey("eggroll.rollsite.polling.authenticator.class")
}


object RollPairConfKeys {
  val EGGROLL_ROLLPAIR_PUTBATCH_EXECUTOR_POOL_MAX_SIZE = ErConfKey("eggroll.rollpair.putbatch.executor.pool.max.size", 600)
  val EGGROLL_ROLLPAIR_PUTBATCH_EXECUTOR_POOL_KEEPALIVE_SEC = ErConfKey("eggroll.rollpair.putbatch.executor.pool.keepalive.sec", 30)
  val EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE = ErConfKey("eggroll.rollpair.default.store.type", "ROLLPAIR_LMDB")
}