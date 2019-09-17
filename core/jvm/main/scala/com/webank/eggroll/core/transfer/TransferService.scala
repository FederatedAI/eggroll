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
 */

package com.webank.eggroll.core.transfer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

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
  override def send(responseObserver: StreamObserver[Transfer.TransferHeader]): StreamObserver[Transfer.Batch] = {
    val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[Transfer.TransferHeader]]

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
  private val map = TrieMap[String, BlockingQueue[Array[Byte]]]()

  def getOrCreateQueue(key: String, size: Int = -1): BlockingQueue[Array[Byte]] = this.synchronized {
    val finalSize = if (size > 0) size else 100

    if (!map.contains(key)) {
      map.put(key, new ArrayBlockingQueue[Array[Byte]](finalSize))
    }
    map(key)
  }

}

class TransferSendCalleeRequestStreamObserver(callerNotifier: ServerCallStreamObserver[Transfer.TransferHeader],
                                              wasReady: AtomicBoolean)
  extends BaseMFCCalleeRequestStreamObserver[Transfer.Batch, Transfer.TransferHeader](callerNotifier, wasReady) {
  private var i = 0

  private var queue: ArrayBlockingQueue[Array[Byte]] = _
  private var inited = false
  private var metadata: Transfer.TransferHeader = _

  override def onNext(value: Transfer.Batch): Unit = {
    if (!inited) {
      queue = GrpcTransferService.getOrCreateQueue(value.getHeader.getTag).asInstanceOf[ArrayBlockingQueue[Array[Byte]]]
      metadata = value.getHeader
      inited = true
    }

    queue.put(value.getData.toByteArray)

    super.onNext(value)
  }

  override def onCompleted(): Unit = {
    callerNotifier.onNext(metadata)
    super.onCompleted()
  }
}