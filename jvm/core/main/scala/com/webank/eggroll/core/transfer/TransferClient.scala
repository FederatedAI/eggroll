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

import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.concurrent.{CountDownLatch, Future, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.{ByteString, UnsafeByteOperations}
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.concurrent.AwaitSettableFuture
import com.webank.eggroll.core.constant.{NetworkConstants, StringConstants, TransferStatus}
import com.webank.eggroll.core.datastructure.Broker
import com.webank.eggroll.core.grpc.client.{GrpcClientContext, GrpcClientTemplate}
import com.webank.eggroll.core.grpc.observer.SameTypeCallerResponseStreamObserver
import com.webank.eggroll.core.grpc.processor.BaseClientCallStreamProcessor
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes._
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErTransferBatch, ErTransferHeader}
import com.webank.eggroll.core.transfer.Transfer.TransferBatch
import com.webank.eggroll.core.util.{GrpcUtils, Logging, ThreadPoolUtils}
import io.grpc.stub.{ClientCallStreamObserver, StreamObserver}

class GrpcTransferClient {
  private val stage = new AtomicInteger(0)
  private val context = new GrpcClientContext[TransferServiceGrpc.TransferServiceStub, Transfer.TransferBatch, Transfer.TransferBatch]
  private val template = new GrpcClientTemplate[TransferServiceGrpc.TransferServiceStub, Transfer.TransferBatch, Transfer.TransferBatch]

  def sendSingle(data: Array[Byte], tag: String, processor: ErProcessor, status: String = StringConstants.EMPTY): Unit = {
    val context = new GrpcClientContext[TransferServiceGrpc.TransferServiceStub, Transfer.TransferBatch, Transfer.TransferBatch]

    val delayedResult = new AwaitSettableFuture[Transfer.TransferBatch]

    context.setStubClass(classOf[TransferServiceGrpc.TransferServiceStub])
      .setServerEndpoint(processor.commandEndpoint)
      .setCallerStreamingMethodInvoker((stub: TransferServiceGrpc.TransferServiceStub,
                                        responseObserver: StreamObserver[Transfer.TransferBatch]) => stub.send(responseObserver))
      .setCallerStreamObserverClassAndInitArgs(classOf[SameTypeCallerResponseStreamObserver[Transfer.TransferBatch, Transfer.TransferBatch]])
      .setRequestStreamProcessorClassAndArgs(classOf[TransferSendStreamProcessor], data, tag, status)

    val template = new GrpcClientTemplate[TransferServiceGrpc.TransferServiceStub, Transfer.TransferBatch, Transfer.TransferBatch]

    template.setGrpcClientContext(context)

    template.initCallerStreamingRpc()
    template.processCallerStreamingRpc()
    template.completeStreamingRpc()
  }

  def init[T](dataBroker: Broker[T], tag: String, processor: ErProcessor, status: String = StringConstants.EMPTY): Unit = {
    if (stage.get() > 0) {
      throw new IllegalStateException("Illegal GrpcTransferClient state: duplicate init")
    }

    context.setStubClass(classOf[TransferServiceGrpc.TransferServiceStub])
      .setServerEndpoint(processor.commandEndpoint)
      .setCallerStreamingMethodInvoker((stub: TransferServiceGrpc.TransferServiceStub,
                                        responseObserver: StreamObserver[Transfer.TransferBatch]) => stub.send(responseObserver))
      .setCallerStreamObserverClassAndInitArgs(classOf[SameTypeCallerResponseStreamObserver[Transfer.TransferBatch, Transfer.TransferBatch]])
      .setRequestStreamProcessorClassAndArgs(classOf[GrpcKvPackingTransferSendStreamProcessor], dataBroker, tag, status)

    template.setGrpcClientContext(context)

    template.initCallerStreamingRpc()
    stage.compareAndSet(0, 1)
  }

  def initForward(dataBroker: Broker[ByteString], tag: String, processor: ErProcessor, status: String = StringConstants.EMPTY): Unit = {
    if (stage.get() > 0) {
      throw new IllegalStateException("Illegal GrpcTransferClient state: duplicate init")
    }

    val metadata = GrpcUtils.toMetadata(Map(StringConstants.TRANSFER_BROKER_NAME -> tag))

    context.setStubClass(classOf[TransferServiceGrpc.TransferServiceStub])
      .setServerEndpoint(processor.transferEndpoint)
      .setCallerStreamingMethodInvoker((stub: TransferServiceGrpc.TransferServiceStub,
                                        responseObserver: StreamObserver[Transfer.TransferBatch]) => stub.send(responseObserver))
      .setCallerStreamObserverClassAndInitArgs(classOf[SameTypeCallerResponseStreamObserver[Transfer.TransferBatch, Transfer.TransferBatch]])
      .setRequestStreamProcessorClassAndArgs(classOf[GrpcForwardingTransferSendStreamProcessor], dataBroker, tag, status)
      .setGrpcMetadata(metadata)


    template.setGrpcClientContext(context)

    template.initCallerStreamingRpc()
    stage.compareAndSet(0, 1)
  }

  def doSend(): Unit = {
    if (stage.get() != 1) {
      throw new IllegalStateException("GrpcTransferClient has not been init yet")
    }

    template.processCallerStreamingRpc()
  }

  def complete(): Unit = {
    if (stage.get() > 2) {
      return
    }
    template.completeStreamingRpc()
  }
}

class GrpcForwardingTransferSendStreamProcessor(clientCallStreamObserver: ClientCallStreamObserver[Transfer.TransferBatch],
                                                dataBroker: Broker[ByteString],
                                                tag: String,
                                                status: String)
  extends BaseClientCallStreamProcessor[Transfer.TransferBatch](clientCallStreamObserver) {

  private val transferHeaderBuilder = Transfer.TransferHeader.newBuilder()
  private val transferBatchBuilder = Transfer.TransferBatch.newBuilder()
  private val dataBuffer = new util.LinkedList[ByteString]()

  override def onProcess(): Unit = {
    while (dataBroker.isReadReady()) {
      dataBroker.drainTo(dataBuffer)

      dataBuffer.forEach(data => {
        val transferBatch = transferBatchBuilder.clear().setHeader(transferHeaderBuilder.setTotalSize(data.size()).setTag(tag)).setData(data)
        clientCallStreamObserver.onNext(transferBatch.build())
      })

    }
  }

  override def onComplete(): Unit = {
    onProcess()
    /*
    if (tag.indexOf() == -1) {
      transferBatchBuilder.clear().setHeader(transferHeaderBuilder.setTotalSize(0).setTag(TransferStatus.TRANSFER_END))
      clientCallStreamObserver.onNext(transferBatchBuilder.build())
    }
     */
    super.onComplete()
  }
}

class GrpcKvPackingTransferSendStreamProcessor(clientCallStreamObserver: ClientCallStreamObserver[Transfer.TransferBatch],
                                               dataBroker: Broker[(Array[Byte], Array[Byte])],
                                               tag: String,
                                               status: String)
  extends BaseClientCallStreamProcessor[Transfer.TransferBatch](clientCallStreamObserver) {

  private val transferHeaderBuilder = Transfer.TransferHeader.newBuilder()
  private val transferBatchBuilder = Transfer.TransferBatch.newBuilder()
  private var directBinPacketBuffer: ByteBuffer = _
  private val binPacketLength = 32 << 20
  private val bufferElementSize = 100
  private var elementCount = 0
  private val dataBuffer = new util.ArrayList[(Array[Byte], Array[Byte])](bufferElementSize)

  /*
   * stages:
   * 0: not inited
   * 1: inited, need to reset directBinPacketBuffer
   * 2: inited, using existing directBinPacketBuffer
   * 3: completed
   * 4: error
   */

  override def onInit(): Unit = {
    super.onInit()

    transferHeaderBuilder.setId(100)
      .setTag(tag)
      .setStatus(status)

    transferBatchBuilder.setHeader(transferHeaderBuilder)
    stage.compareAndSet(0, 1)

    reset()
  }

  // after reset, 8 + 4 + 4 + 4 = 20 bytes will be occupied
  private def reset(): Unit = {
    if (stage.get() != 1) {
      return
    }
    directBinPacketBuffer = ByteBuffer.allocateDirect(binPacketLength)
    directBinPacketBuffer.order(ByteOrder.LITTLE_ENDIAN)
    directBinPacketBuffer.put(NetworkConstants.TRANSFER_PROTOCOL_MAGIC_NUMBER)   // magic num
    directBinPacketBuffer.put(NetworkConstants.TRANSFER_PROTOCOL_VERSION)     // protocol version
    directBinPacketBuffer.putInt(0)   // header length
    directBinPacketBuffer.putInt(0)   // body length

    elementCount = 0
    stage.set(2)
  }

  private def getBufferSize(): Int = binPacketLength - directBinPacketBuffer.remaining()

  override def onProcess(): Unit = {
    //super.onProcess()

    while (dataBroker.isReadReady()) {
      dataBuffer.clear()
      dataBroker.drainTo(dataBuffer, bufferElementSize)

      dataBuffer.forEach(e => {
        val kLen = e._1.length
        val vLen = e._2.length

        if (directBinPacketBuffer.remaining() - kLen - vLen - 8 < 0) {
          directBinPacketBuffer.flip()
          val data = UnsafeByteOperations.unsafeWrap(directBinPacketBuffer)

          // set final body length
          directBinPacketBuffer.putInt(16, data.size())
          transferHeaderBuilder.setTotalSize(data.size())
          transferBatchBuilder.setHeader(transferHeaderBuilder)
            .setData(data)
            .setBatchSize(elementCount)

          clientCallStreamObserver.onNext(transferBatchBuilder.build())
          stage.compareAndSet(2, 1)
        }

        if (stage.get() == 1) {
          reset()
        }

        directBinPacketBuffer.putInt(kLen)    // key length
        directBinPacketBuffer.put(e._1)       // key
        directBinPacketBuffer.putInt(vLen)    // value length
        directBinPacketBuffer.put(e._2)       // value
        elementCount += 1
      })
    }
  }

  override def onComplete(): Unit = {
    onProcess()

    if (dataBroker.isClosable() && getBufferSize() > 0) {
      directBinPacketBuffer.flip()
      val data = UnsafeByteOperations.unsafeWrap(directBinPacketBuffer)

      // set final body length
      directBinPacketBuffer.putInt(16, data.size())
      transferHeaderBuilder.setTotalSize(data.size())
          .setStatus(TransferStatus.TRANSFER_END)
      transferBatchBuilder.setHeader(transferHeaderBuilder)
        .setData(data)
        .setBatchSize(getBufferSize())

      clientCallStreamObserver.onNext(transferBatchBuilder.build())
    }
    stage.set(3)

    super.onComplete()
  }
}


class InternalTransferClient(defaultEndpoint: ErEndpoint) extends Logging {
  def send(requests: Iterator[Transfer.TransferBatch],
           endpoint: ErEndpoint = defaultEndpoint,
           options: Map[String, String] = Map.empty): Transfer.TransferBatch = {
    val channel = GrpcClientUtils.getChannel(endpoint, false, options)
    val stub = TransferServiceGrpc.newStub(channel)
    var result: Transfer.TransferBatch = null
    val finishLatch = new CountDownLatch(1)

    val streamObserver = stub.send(new StreamObserver[Transfer.TransferBatch] {
      // define what to do when server responds
      override def onNext(v: Transfer.TransferBatch): Unit = {
        result = v
        logInfo(s"response metadata: ${result}")
      }

      // define what to do when server gives error - backward propagation
      override def onError(throwable: Throwable): Unit = {
        // process error here
        finishLatch.countDown()
      }

      // define what to do when server finishes
      override def onCompleted(): Unit = {
        // process finish here
        finishLatch.countDown()
      }
    })

    for (request <- requests) {
      streamObserver.onNext(request)
    }

    streamObserver.onCompleted()
    finishLatch.await()

    result
  }

  def sendAsync(requests: Iterator[Transfer.TransferBatch],
                endpoint: ErEndpoint = defaultEndpoint,
                options: Map[String, String] = Map.empty): Future[Transfer.TransferBatch] = {
    InternalTransferClient.internalTransferClientExecutor.submit(() => {
      send(requests, endpoint, options)
    })
  }
}

object InternalTransferClient {
  val internalTransferClientExecutor: ThreadPoolExecutor =
    ThreadPoolUtils.newCachedThreadPool("internal-transfer-client")
}

class TransferSendStreamProcessor(clientCallStreamObserver: ClientCallStreamObserver[Transfer.TransferBatch],
                                  data: Array[Byte],
                                  tag: String,
                                  status: String)
  extends BaseClientCallStreamProcessor[Transfer.TransferBatch](clientCallStreamObserver) {
  override def onProcess(): Unit = {
    val finalData = if (data != null) data else Array.emptyByteArray
    val batch = ErTransferBatch(
      header = ErTransferHeader(
        id = 100,
        tag = tag,
        totalSize = finalData.size,
        status = status),
      data = finalData)
    clientCallStreamObserver.onNext(batch.toProto())
  }
}