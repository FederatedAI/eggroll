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

package com.webank.eggroll.core.transfer

import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.webank.eggroll.core.constant.CoreConfKeys
import com.webank.eggroll.core.util.{FileSystemUtils, Logging, ThreadPoolUtils}
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyServerBuilder}
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth
import io.grpc.{BindableService, Server}


object GrpcServerUtils extends Logging {
  private def modulePrefix = "[CORE][SERVER]"

  def createServer(host: String,
                   port: Int,
                   grpcServices: List[BindableService],
                   options: Map[String, String] = Map()): Server = {
    if (port < 0) throw new IllegalArgumentException(s"${modulePrefix} cannot listen to port <= 0")

    val addr = new InetSocketAddress(host, port)

    val nettyServerBuilder = NettyServerBuilder.forAddress(addr)

    grpcServices.foreach(s => nettyServerBuilder.addService(s))

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logInfo(s"*** shutting down gRPC server in shutdown hook. host: ${host}, port ${port} ***")
        this.interrupt()
        logInfo(s"*** server shut down. host: ${host}, port ${port} ***")
      }
    })

    val maxConcurrentCallPerConnection = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION.getWith(options).toInt
    val maxInboundMessageSize = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.getWith(options).toInt
    val maxInboundMetadataSize = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE.getWith(options).toInt
    val flowControlWindow = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW.getWith(options).toInt
    val channelKeepAliveTimeSec = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC.getWith(options).toLong
    val channelKeepAliveTimeoutSec = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC.getWith(options).toLong
    val channelPermitKeepAliveTime = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC.getWith(options).toLong
    val channelKeepAliveWithoutCallsEnabled = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED.getWith(options).toBoolean
    val maxConnectionIdle = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC.getWith(options).toLong
    val maxConnectionAge = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC.getWith(options).toLong
    val maxConnectionAgeGrace = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC.getWith(options).toLong

    nettyServerBuilder.
      executor(ThreadPoolUtils.newCachedThreadPool(s"grpc-server-${port}"))
      .maxConcurrentCallsPerConnection(maxConcurrentCallPerConnection)
      .maxInboundMessageSize(maxInboundMessageSize)
      .maxInboundMetadataSize(maxInboundMetadataSize)
      .flowControlWindow(flowControlWindow)
      .keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS)
      .keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS)
      .permitKeepAliveTime(channelPermitKeepAliveTime, TimeUnit.SECONDS)
      .permitKeepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled)
      .maxConnectionIdle(maxConnectionIdle, TimeUnit.SECONDS)
      .maxConnectionAge(maxConnectionAge, TimeUnit.SECONDS)
      .maxConnectionAgeGrace(maxConnectionAgeGrace, TimeUnit.SECONDS)

    val secureClusterEnabled = CoreConfKeys.CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED.getWith(options).toBoolean
    if (secureClusterEnabled) {
      val caCrtPath = FileSystemUtils.stripParentDirReference(CoreConfKeys.CONFKEY_CORE_SECURITY_CA_CRT_PATH.getWith(options))
      val keyCrtPath = FileSystemUtils.stripParentDirReference(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_CRT_PATH.getWith(options))
      val keyPath = FileSystemUtils.stripParentDirReference(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_PATH.getWith(options))

      val clientAuthEnabled = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_AUTH_ENABLED.getWith(options).toBoolean
      val sslSessionTimeout = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC.getWith(options).toLong
      val sslSessionCacheSize = CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE.getWith(options).toLong

      val caCrt = new File(caCrtPath)
      val keyCrt = new File(keyCrtPath)
      val key = new File(keyPath)

      val sslContextBuilder = GrpcSslContexts.forServer(keyCrt, key).trustManager(caCrt).sessionTimeout(sslSessionTimeout).sessionCacheSize(sslSessionCacheSize)

      if (clientAuthEnabled) sslContextBuilder.clientAuth(ClientAuth.REQUIRE)
      else sslContextBuilder.clientAuth(ClientAuth.OPTIONAL)

      nettyServerBuilder.sslContext(sslContextBuilder.build)
      logInfo(s"${port} starting in secure mode. " +
        s"server private key path: ${key.getAbsolutePath}, " +
        s"key crt path: ${keyCrt.getAbsoluteFile}, " +
        s"ca crt path: ${caCrt.getAbsolutePath}")
    } else {
      logInfo(s"${port} starting in insecure mode")
    }

    nettyServerBuilder.build()
  }
}
