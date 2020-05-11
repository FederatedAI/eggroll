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

import java.util.concurrent.atomic.AtomicBoolean

import com.webank.eggroll.core.constant.{CoreConfKeys, TransferStatus}
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.util.{GrpcCalleeStreamObserver, Logging}
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
  def send(any: Any)
}

// TODO:1: grpc channel pool per object
// TODO:1: grpc error info with self / peer ip / port
object TransferClient {
  def apply(options: Map[String, String]): TransferClient = ???
}


class GrpcTransferService private extends TransferService with Logging {

  override def start(conf: RuntimeErConf): Unit = {
    val host = CoreConfKeys.CONFKEY_CORE_GRPC_TRANSFER_SERVER_HOST.get()
    val port = CoreConfKeys.CONFKEY_CORE_GRPC_TRANSFER_SERVER_PORT.get().toInt

    GrpcServerUtils.createServer(
      host = host,
      port = port,
      grpcServices = List(new TransferServiceGrpcImpl()))
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