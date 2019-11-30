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
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import com.webank.eggroll.core.constant.{StringConstants, TransferStatus}
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.grpc.observer.BaseMFCCalleeRequestStreamObserver
import com.webank.eggroll.core.grpc.server.GrpcServerWrapper
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.collection.concurrent.TrieMap

trait TransferService {
}

class GrpcTransferService extends TransferServiceGrpc.TransferServiceImplBase {
  private val grpcServerWrapper = new GrpcServerWrapper()

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

    new TransferSendCalleeRequestStreamObserver(serverCallStreamObserver, wasReady)
  }
}

object GrpcTransferService {
  private val dataBuffer = TrieMap[String, Broker[Transfer.TransferBatch]]()

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

class TransferSendCalleeRequestStreamObserver(callerNotifier: ServerCallStreamObserver[Transfer.TransferBatch],
                                              wasReady: AtomicBoolean)
  extends BaseMFCCalleeRequestStreamObserver[Transfer.TransferBatch, Transfer.TransferBatch](callerNotifier, wasReady) {
  private var i = 0

  private var broker: Broker[Transfer.TransferBatch] = _
  private var inited = false
  private var responseHeader: Transfer.TransferHeader = _

  override def onNext(value: Transfer.TransferBatch): Unit = {
    if (!inited) {
      broker = GrpcTransferService.getOrCreateBroker(value.getHeader.getTag)
      responseHeader = value.getHeader
      inited = true
    }

    broker.put(value)
    if (value.getHeader.getStatus.equals(TransferStatus.TRANSFER_END)) {
      broker.signalWriteFinish()
    }

    super.onNext(value)
  }

  override def onCompleted(): Unit = {
    callerNotifier.onNext(Transfer.TransferBatch.newBuilder().setHeader(responseHeader).build())
    super.onCompleted()
  }
}