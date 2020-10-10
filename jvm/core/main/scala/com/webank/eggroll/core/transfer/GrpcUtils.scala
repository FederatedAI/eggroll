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
import java.util.concurrent.{Callable, ExecutionException, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalNotification}
import com.webank.eggroll.core.constant.{CoreConfKeys, ModuleConstants}
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.retry.RetryException
import com.webank.eggroll.core.retry.factory.{AttemptOperations, RetryerBuilder, StopStrategies, WaitTimeStrategies}
import com.webank.eggroll.core.util.{FileSystemUtils, Logging, ThreadPoolUtils}
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NegotiationType, NettyChannelBuilder, NettyServerBuilder}
import io.grpc.netty.shaded.io.netty.handler.ssl.{ClientAuth, SslContext}
import io.grpc.{BindableService, ManagedChannel, Server, ServerServiceDefinition}
import org.apache.commons.lang3.StringUtils


object GrpcServerUtils extends Logging {
  private def modulePrefix = "[CORE][SERVER]"

  def createServer(host: String = "0.0.0.0",
                   port: Int = 0,
                   grpcServices: List[BindableService] = List.empty,
                   bindServices: List[ServerServiceDefinition] = List.empty,
                   options: Map[String, String] = Map.empty): Server = {
    if (port < 0) throw new IllegalArgumentException(s"${modulePrefix} cannot listen to port <= 0")
    if (grpcServices.isEmpty) throw new IllegalArgumentException("grpc services cannot be empty")

    val addr = new InetSocketAddress(host, port)

    val nettyServerBuilder = NettyServerBuilder.forAddress(addr)

    grpcServices.foreach(s => nettyServerBuilder.addService(s))
    bindServices.foreach(s => nettyServerBuilder.addService(s))

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

    nettyServerBuilder
      .executor(ThreadPoolUtils.newCachedThreadPool(s"grpc-server-${port}"))
      .maxConcurrentCallsPerConnection(maxConcurrentCallPerConnection)
      .maxInboundMessageSize(maxInboundMessageSize)
      .maxInboundMetadataSize(maxInboundMetadataSize)
      .flowControlWindow(flowControlWindow)

      if (channelKeepAliveTimeSec > 0) nettyServerBuilder.keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS)
      if (channelKeepAliveTimeoutSec > 0) nettyServerBuilder.keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS)
      if (channelPermitKeepAliveTime > 0) nettyServerBuilder.permitKeepAliveTime(channelPermitKeepAliveTime, TimeUnit.SECONDS)
      if (channelKeepAliveWithoutCallsEnabled) nettyServerBuilder.permitKeepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled)
      if (maxConnectionIdle > 0) nettyServerBuilder.maxConnectionIdle(maxConnectionIdle, TimeUnit.SECONDS)
      if (maxConnectionAge > 0) nettyServerBuilder.maxConnectionAge(maxConnectionAge, TimeUnit.SECONDS)
      if (maxConnectionAgeGrace > 0) nettyServerBuilder.maxConnectionAgeGrace(maxConnectionAgeGrace, TimeUnit.SECONDS)

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
      logInfo(s"gRPC server at port=${port} starting in secure mode. " +
        s"server private key path: ${key.getAbsolutePath}, " +
        s"key crt path: ${keyCrt.getAbsoluteFile}, " +
        s"ca crt path: ${caCrt.getAbsolutePath}")
    } else {
      logInfo(s"gRPC server at ${port} starting in insecure mode")
    }

    nettyServerBuilder.build()
  }
}

object GrpcClientUtils extends Logging {

  val maximumSize: Long = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE.get().toLong
  val expireTimeout: Long = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC.get().toLong
  val channelTerminationAwaitTimeout: Long = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC.get.toLong
  private val cacheBuilder = CacheBuilder.newBuilder()
    .maximumSize(maximumSize)
    .expireAfterAccess(expireTimeout, TimeUnit.SECONDS)
    .recordStats()
    .removalListener((notification: RemovalNotification[ErEndpoint, ManagedChannel]) => {
      val endpoint = notification.getKey
      val managedChannel = notification.getValue
      if (managedChannel != null) if (!managedChannel.isShutdown || !managedChannel.isTerminated) managedChannel.shutdown

      logDebug(s"[CHANNEL][REMOVAL] removing for endpoint=${endpoint}, id=${Integer.toHexString(endpoint.hashCode())}. reason=${notification.getCause.name()}")
    })
  private val insecureChannelCache: LoadingCache[ErEndpoint, ManagedChannel] = cacheBuilder
    .build(new CacheLoader[ErEndpoint, ManagedChannel]() {
      override def load(endpoint: ErEndpoint): ManagedChannel = {
        logDebug(s"[CHANNEL][INSECURE] creating for endpoint=${endpoint}, id=${Integer.toHexString(endpoint.hashCode())}")
        createChannel(endpoint, isSecureChannel = false)
      }
    })

  private val secureChannelCache: LoadingCache[ErEndpoint, ManagedChannel] = cacheBuilder
    .build(new CacheLoader[ErEndpoint, ManagedChannel]() {
      override def load(endpoint: ErEndpoint): ManagedChannel = {
        logDebug(s"[CHANNEL][SECURE] creating for endpoint=${endpoint}, id=${Integer.toHexString(endpoint.hashCode())}")
        createChannel(endpoint, isSecureChannel = true)
      }
    })

  private val channelWithBuckets = "[CHANNEL]"
  private val removeWithBuckets = "[REMOVE]"
  private val createWithBuckets = "[CREATE]"
  private val prefix = ModuleConstants.CORE_WITH_BRACKETS + channelWithBuckets

  def getChannelCacheSize(isSecure: Boolean): Long =
    if (isSecure) secureChannelCache.size() else insecureChannelCache.size()

  private def createChannel(endpoint: ErEndpoint,
                            isSecureChannel: Boolean = false,
                            options: Map[String, String] = Map.empty): ManagedChannel = {
    val channelKeepAliveTimeSec = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC.getWith(options).toLong
    val channelKeepAliveTimeoutSec = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC.getWith(options).toLong
    val channelKeepAliveWithoutCallsEnabled = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED.getWith(options).toBoolean
    val channelIdleTimeoutSec = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC.getWith(options).toInt
    val channelPerRpcBufferLimit = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT.getWith(options).toInt
    val channelFlowControlWindow = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW.getWith(options).toInt
    val channelMaxInboundMessageSize = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.getWith(options).toInt
    val channelMaxInboundMetadataSize = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE.getWith(options).toInt
    val channelRetryBufferSize = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE.getWith(options).toInt
    val channelMaxRetryAttempts = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS.getWith(options).toInt
    val channelExecutorPoolSize = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE.getWith(options).toInt
    val caCrtPath = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH.getWith(options)
    var caCrt: File = null
    if (isSecureChannel) {
      if (StringUtils.isBlank(caCrtPath)) throw new IllegalArgumentException("secure channel required but no ca crt conf found")
      caCrt = new File(caCrtPath)
      if (!caCrt.exists) throw new IllegalArgumentException(s"ca crt at path: ${caCrtPath} not found")
    }
    val builder = NettyChannelBuilder
      .forAddress(endpoint.host, endpoint.port)
      .perRpcBufferLimit(channelPerRpcBufferLimit)
      .flowControlWindow(channelFlowControlWindow)
      .maxInboundMessageSize(channelMaxInboundMessageSize)
      .maxInboundMetadataSize(channelMaxInboundMetadataSize)

    if (channelIdleTimeoutSec > 0) builder.idleTimeout(channelIdleTimeoutSec, TimeUnit.SECONDS)
    if (channelKeepAliveTimeSec > 0) builder.keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS)
    if (channelKeepAliveTimeoutSec > 0) builder.keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS)
    if (channelKeepAliveWithoutCallsEnabled) builder.keepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled)

      if (channelMaxRetryAttempts > 0) {
        builder.enableRetry()
          .retryBufferSize(channelRetryBufferSize)
          .maxRetryAttempts(channelMaxRetryAttempts)
      } else {
        builder.disableRetry()
      }

    if (isSecureChannel) {
      var sslContext: SslContext = null
      val sslSessionTimeout = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC.getWith(options).toLong
      val sslSessionCacheSize = CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE.getWith(options).toLong
      val keyCrtPath = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_KEY_CRT_PATH.getWith(options)
      val keyPath = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_KEY_PATH.getWith(options)
      val sslContextBuilder = GrpcSslContexts
        .forClient
        .trustManager(caCrt)
        .sessionTimeout(sslSessionTimeout)
        .sessionCacheSize(sslSessionCacheSize)

      if (StringUtils.isNoneBlank(keyCrtPath, keyPath)) {
        val serverCrt = new File(keyCrtPath)
        val serverKey = new File(keyPath)
        sslContextBuilder.keyManager(serverCrt, serverKey)
      }
      sslContext = sslContextBuilder.build

      builder
        .sslContext(sslContext)
        .useTransportSecurity
        .negotiationType(NegotiationType.TLS)
    } else {
      builder
        .negotiationType(NegotiationType.PLAINTEXT)
        .usePlaintext
    }
    builder.build
  }

  private def getChannelInternal(endpoint: ErEndpoint, isSecureChannel: Boolean, options: Map[String, String] = Map.empty): ManagedChannel = {
    var result: ManagedChannel = null
    val cache = if (isSecureChannel) secureChannelCache else insecureChannelCache
    result = cache.getUnchecked(endpoint)
    if (result == null || result.isShutdown || result.isTerminated) {
      if (isSecureChannel)
        cache.invalidate(result)
      result = cache.get(endpoint)
    }

    result
  }

  def getChannel(endpoint: ErEndpoint,
                 isSecureChannel: Boolean = false,
                 options: Map[String, String] = Map.empty): ManagedChannel = {
    var result: ManagedChannel = null
    val fixedWaitTime = CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS.getWith(options).toLong
    val maxAttempts = CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS.getWith(options).toInt
    val attemptTimeout = CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS.getWith(options).toLong
    val retryer = RetryerBuilder.newBuilder[ManagedChannel]
      .withWaitTimeStrategy(WaitTimeStrategies.fixedWaitTime(fixedWaitTime))
      .withStopStrategy(StopStrategies.stopAfterMaxAttempt(maxAttempts))
      .withAttemptOperation(AttemptOperations.fixedTimeLimit[ManagedChannel](attemptTimeout, TimeUnit.MILLISECONDS))
      .retryIfAnyException
      .build

    val getUsableChannel = new Callable[ManagedChannel]() {
      override def call(): ManagedChannel =
        getChannelInternal(endpoint, isSecureChannel)
    }

    try result = retryer.call(getUsableChannel)
    catch {
      case e: ExecutionException =>
        Thread.currentThread.interrupt()
        throw new RuntimeException(e)
      case e: RetryException =>
        logError(s"{} Error getting ManagedChannel after retries: ${prefix}", e)
        throw e
    }
    result
  }

  def shutdownNow() = {
    secureChannelCache.asMap().keySet().toArray().foreach(key => {
      val managedChannel = insecureChannelCache.asMap().get(key)
      logDebug(s"shutting down secure channel=${managedChannel}")
      managedChannel.shutdownNow()
    })

    insecureChannelCache.asMap().keySet().toArray().foreach(key => {
      val managedChannel = insecureChannelCache.asMap().get(key)
      logDebug(s"shutting down insecure channel=${managedChannel}")
      managedChannel.shutdownNow()
    })
  }
}