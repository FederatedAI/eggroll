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
import java.util.concurrent.atomic.AtomicBoolean

import com.webank.eggroll.core.constant.{CoreConfKeys, StringConstants, TransferStatus}
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.session.{RuntimeErConf, StaticErConf}
import com.webank.eggroll.core.util.{FileSystemUtils, GrpcCalleeStreamObserver, Logging, ThreadPoolUtils}
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyServerBuilder}
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.collection.concurrent.TrieMap


trait TransferService {
  def start(conf: RuntimeErConf)
}


object TransferService {
  private val dataBuffer = TrieMap[String, Broker[Transfer.TransferBatch]]()

  def apply(options: Map[String, String]): TransferService = ???

  def getOrCreateBroker(key: String,
                        maxCapacity: Int = -1,
                        writeSignals: Int = 1): Broker[Transfer.TransferBatch] = this.synchronized {
    val finalSize = if (maxCapacity > 0) maxCapacity else 100

    if (!dataBuffer.contains(key)) {
      dataBuffer.put(key,
        new LinkedBlockingBroker[Transfer.TransferBatch](maxCapacity = finalSize, writeSignals = writeSignals, name = key))
    }
    dataBuffer(key)
  }

  def getBroker(key: String): Broker[Transfer.TransferBatch] = if (dataBuffer.contains(key)) dataBuffer(key) else null

  def containsBroker(key: String): Boolean = dataBuffer.contains(key)
}


trait TransferClient {
  def send()
}


object TransferClient {
  def apply(options: Map[String, String]): TransferClient = ???
}


class GrpcTransferService private extends TransferService with Logging {

  override def start(conf: RuntimeErConf): Unit = {
    val host = conf.getString(CoreConfKeys.CONFKEY_CORE_GRPC_TRANSFER_SERVER_HOST, "localhost")
    val port = conf.getInt(CoreConfKeys.CONFKEY_CORE_GRPC_TRANSFER_SERVER_PORT, 0)

    if (port < 0) throw new IllegalArgumentException("cannot listen to port < 0")

    val addr = new InetSocketAddress(host, port)
    val serverBuilder = NettyServerBuilder.forAddress(addr)
        .addService(new TransferServiceGrpcImpl)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logInfo(s"*** shutting down gRPC server in shutdown hook. addr: ${host}:${port} ***")
        this.interrupt()
        logInfo(s"*** server shut down. addr: ${host}:${port} ***")
      }
    })

    val maxConcurrentCallPerConnection = StaticErConf.getInt(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION, 10000)
    val maxInboundMessageSize = StaticErConf.getInt(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE, 32 << 20)
    val flowControlWindow = StaticErConf.getInt(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW, 16 << 20)
    val channelKeepAliveTimeSec = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC, 300L)
    val channelKeepAliveTimeoutSec = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC, 3600L)
    val channelPermitKeepAliveTime = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC, 1L)
    val channelKeepAliveWithoutCallsEnabled = StaticErConf.getBoolean(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED, true)
    val maxConnectionIdle = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC, 300L)
    val maxConnectionAge = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC, 86400L)
    val maxConnectionAgeGrace = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC, 86400L)

    serverBuilder.executor(ThreadPoolUtils.defaultThreadPool).maxConcurrentCallsPerConnection(maxConcurrentCallPerConnection).maxInboundMessageSize(maxInboundMessageSize).flowControlWindow(flowControlWindow).keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS).keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS).permitKeepAliveTime(channelPermitKeepAliveTime, TimeUnit.SECONDS).permitKeepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled).maxConnectionIdle(maxConnectionIdle, TimeUnit.SECONDS).maxConnectionAge(maxConnectionAge, TimeUnit.SECONDS).maxConnectionAgeGrace(maxConnectionAgeGrace, TimeUnit.SECONDS)

    if (conf.getBoolean(CoreConfKeys.CONFKEY_CORE_GRPC_TRANSFER_SECURE_SERVER_ENABLED, false)) {
      val caCrtPath = FileSystemUtils.stripParentDirReference(StaticErConf.getString(CoreConfKeys.CONFKEY_CORE_SECURITY_CA_CRT_PATH, StringConstants.EMPTY))
      val keyCrtPath = FileSystemUtils.stripParentDirReference(StaticErConf.getString(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_CRT_PATH, StringConstants.EMPTY))
      val keyPath = FileSystemUtils.stripParentDirReference(StaticErConf.getString(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_PATH, StringConstants.EMPTY))
      val secureClusterEnabled = StaticErConf.getBoolean(CoreConfKeys.CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED, false)
      val sslSessionTimeout = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC, 3600 << 4)
      val sslSessionCacheSize = StaticErConf.getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE, 65536L)
      val caCrt = new File(caCrtPath)
      val keyCrt = new File(keyCrtPath)
      val key = new File(keyPath)
      val sslContextBuilder = GrpcSslContexts.forServer(keyCrt, key).trustManager(caCrt).sessionTimeout(sslSessionTimeout).sessionCacheSize(sslSessionCacheSize)
      if (secureClusterEnabled) sslContextBuilder.clientAuth(ClientAuth.REQUIRE)
      else sslContextBuilder.clientAuth(ClientAuth.OPTIONAL)
      serverBuilder.sslContext(sslContextBuilder.build)

      logInfo(s"starting in secure mode. server key path: ${keyPath}, key crt path: ${keyCrtPath}, ca crt path: ${caCrtPath}")
    }
    else {
      logInfo("starting in insecure mode.")
    }

    return serverBuilder.build
  }
}


class TransferServiceGrpcImpl extends TransferServiceGrpc.TransferServiceImplBase {

  /**
   */
  override def send(responseObserver: StreamObserver[Transfer.TransferBatch]): StreamObserver[Transfer.TransferBatch] = {
    val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[Transfer.TransferBatch]]

    serverCallStreamObserver.disableAutoInboundFlowControl()

    val wasReady = new AtomicBoolean(false)

    serverCallStreamObserver.setOnReadyHandler(() => {
      if (serverCallStreamObserver.isReady && wasReady.compareAndSet(false, true)) {
        serverCallStreamObserver.request(1)
      }
    })

    new TransferCallee(serverCallStreamObserver, wasReady)
  }
}


private class TransferCallee(caller: ServerCallStreamObserver[Transfer.TransferBatch],
                             wasReady: AtomicBoolean)
  extends GrpcCalleeStreamObserver[Transfer.TransferBatch, Transfer.TransferBatch](caller) {
  private var i = 0

  private var broker: Broker[Transfer.TransferBatch] = _
  private var inited = false
  private var responseHeader: Transfer.TransferHeader = _

  override def onNext(value: Transfer.TransferBatch): Unit = {
    if (!inited) {
      broker = TransferService.getOrCreateBroker(value.getHeader.getTag)
      responseHeader = value.getHeader
      inited = true
    }

    broker.put(value)
    if (value.getHeader.getStatus.equals(TransferStatus.TRANSFER_END)) {
      broker.signalWriteFinish()
    }

    if (caller.isReady) caller.request(1) else wasReady.set(false)
  }

  override def onCompleted(): Unit = {
    caller.onNext(Transfer.TransferBatch.newBuilder().setHeader(responseHeader).build())
    super.onCompleted()
  }
}