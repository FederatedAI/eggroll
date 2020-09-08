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

package com.webank.eggroll.rollsite

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.constant.{CoreConfKeys, RollSiteConfKeys}
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import com.webank.eggroll.rollsite.PollingResults.errorPoison
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.commons.lang3.StringUtils


object PollingMethods {
  val PUSH = "push"
  val FINISH_PUSH = "finish_push"
  val UNARY_CALL = "unary_call"
  val FINISH_UNARY_CALL = "finish_unary_call"
  val COMPLETED_POISON = "completed_poison"
  val NO_DATA_POISON = "no_data_poison"
  val ERROR_POISON = "error_poison"
}

object LongPollingClient {
  private val defaultPollingReqMetadata: Proxy.Metadata = Proxy.Metadata.newBuilder()
    .setDst(
      Proxy.Topic.newBuilder()
        .setPartyId(RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()))
    .build()

  val initPollingFrameBuilder: Proxy.PollingFrame.Builder = Proxy.PollingFrame.newBuilder().setMetadata(defaultPollingReqMetadata)

  private val pollingSemaphore = new Semaphore(RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_CONCURRENCY.get().toInt)

  def acquireSemaphore(): Unit = {
    LongPollingClient.pollingSemaphore.acquire()
  }

  def releaseSemaphore(): Unit = {
    LongPollingClient.pollingSemaphore.release()
  }
}

class LongPollingClient extends Logging {
  def polling(): Unit = {
    LongPollingClient.acquireSemaphore()

    try {
      val endpoint = Router.query("default").point
      var isSecure = Router.query("default").isSecure
      val caCrt = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH.get()

      // use secure channel conditions:
      // 1 include crt file.
      // 2 point is secure
      isSecure = if (!StringUtils.isBlank(caCrt)) isSecure else false
      val channel = GrpcClientUtils.getChannel(endpoint, isSecure)
      val stub = DataTransferServiceGrpc.newStub(channel)
      val pollingResults = new PollingResults()
      val dispatchPollingRespSO = new DispatchPollingRespSO(pollingResults)
      //val dispatchPollingRespSO = new MockPollingRespSO(pollingResults)
      val pollingReqSO = stub.polling(dispatchPollingRespSO)

      pollingReqSO.onNext(
        LongPollingClient.initPollingFrameBuilder
          .build())

      try {
        var finished = false
        var req: Proxy.PollingFrame = null
        while (!finished) {
          req = pollingResults.next()

          req.getMethod match {
            case PollingMethods.PUSH | PollingMethods.UNARY_CALL =>
              pollingReqSO.onNext(req)
            case PollingMethods.COMPLETED_POISON =>
              pollingReqSO.onCompleted()
              finished = true
            case PollingMethods.NO_DATA_POISON =>
              throw new IllegalStateException("polling timeout with no data")
            case PollingMethods.ERROR_POISON =>
              throw pollingResults.getError()
            case _ =>
              throw new NotImplementedError(s"received unknown method: ${req.getMethod}")
          }
        }
      } catch {
        case t: Throwable =>
          logError("polling with error", t)
          pollingReqSO.onError(TransferExceptionUtils.throwableToException(t))
      }
      //pollingResults.await()

      // TODO:0: configurable
    } catch {
      case t: Throwable =>
        logError("polling failed", t)
        Thread.sleep(1000)
    }
  }

  def pollingForever(): Unit = {
    while (true) {
      polling()
    }
  }
}

class PollingExchanger() {
  val reqQ = new SynchronousQueue[Proxy.PollingFrame]()
  val respQ = new SynchronousQueue[Proxy.PollingFrame]()
  private var method: String = _
  private val methodLatch = new CountDownLatch(1)

  def setMethod(method: String): Unit = {
    if (StringUtils.isBlank(this.method)) {
      this.method = method
      methodLatch.countDown()
    }
  }

  def waitMethod(): String = {
    methodLatch.await()
    method
  }
}

object PollingHelper {
  val pollingExchangerQueue = new LinkedBlockingQueue[PollingExchanger]()
}

class PollingResults() extends Iterator[Proxy.PollingFrame] with Logging {
  private val q = new LinkedBlockingQueue[Proxy.PollingFrame]()
  private val error: AtomicReference[Throwable] = new AtomicReference[Throwable](null)

  def put(f: Proxy.PollingFrame): Unit = {
    q.put(f)
  }

  def setFinish(): Unit = q.put(PollingResults.completedPoison)

  def setError(t: Throwable): Unit = {
    this.error.compareAndSet(null, t)
    q.put(errorPoison)
  }

  def getError(): Throwable = this.error.get()

  override def hasNext: Boolean = true

  override def next(): Proxy.PollingFrame = {
    // TODO:0: Configurable
    val result: Proxy.PollingFrame = q.poll(5, TimeUnit.MINUTES)

    if (result == null) {
      q.put(PollingResults.noDataPoison)
    }

    result
  }
}

object PollingResults {
  val completedPoison: Proxy.PollingFrame = Proxy.PollingFrame.newBuilder().setMethod(PollingMethods.COMPLETED_POISON).build()
  val noDataPoison: Proxy.PollingFrame = Proxy.PollingFrame.newBuilder().setMethod(PollingMethods.NO_DATA_POISON).build()
  val errorPoison: Proxy.PollingFrame = Proxy.PollingFrame.newBuilder().setMethod(PollingMethods.ERROR_POISON).build()
}

/***************** STREAM OBSERVERS *****************/

/**
 * Position: Exchange point
 * Side: Server
 * Functionalities: Handles requests from polling client
 *
 * Steps:
 * 1. 1st packet: extracts dstPartyId, and puts it in PollingHelper.pullSOs
 * 2. 2nd packet and on: push / unaryCall response.
 *
 * @param eggSiteServicerPollingRespSO
 */
class DispatchPollingReqSO(eggSiteServicerPollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _
  private var pollingExchanger: PollingExchanger = _

  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    pollingExchanger = new PollingExchanger()
    PollingHelper.pollingExchangerQueue.put(pollingExchanger)

    // synchronise point for incoming push / unary_call request
    val method = pollingExchanger.waitMethod()

    method match {
      case PollingMethods.PUSH =>
        delegateSO = new PushPollingReqSO(eggSiteServicerPollingRespSO, pollingExchanger)
      case PollingMethods.UNARY_CALL =>
        delegateSO = new UnaryCallPollingReqSO(eggSiteServicerPollingRespSO, pollingExchanger)
      case _ =>
        val e = new NotImplementedError(s"method ${method} not supported")
        logError(e)
        onError(e)
    }

    inited = true
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    ensureInited(req)
    delegateSO.onNext(req)
  }

  override def onError(t: Throwable): Unit = {
    delegateSO.onError(t)
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
  }
}


class UnaryCallPollingReqSO(eggSiteServicerPollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame],
                            pollingExchanger: PollingExchanger)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _
  private var inited = false

  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingReqSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    inited = true
    logDebug(s"UnaryCallPollingReqSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingReqSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")

    var batch: Proxy.PollingFrame = null
    req.getSeq match {
      case 0L =>
        batch = pollingExchanger.respQ.take()
        ensureInited(batch)

        eggSiteServicerPollingRespSO.onNext(batch)
      case 1L =>
        pollingExchanger.reqQ.put(Proxy.PollingFrame.newBuilder().setPacket(req.getPacket).build())
      case _ =>
        val t: Throwable = new IllegalStateException(s"invalid seq=${req.getSeq} for rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
        onError(t)
    }
    logTrace(s"UnaryCallPollingReqSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"UnaryCallPollingReqSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    eggSiteServicerPollingRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"UnaryCallPollingReqSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"UnaryCallPollingReqSO.onCompleted calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    eggSiteServicerPollingRespSO.onCompleted()
    logTrace(s"UnaryCallPollingReqSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

// server side. processes push polling req
class PushPollingReqSO(val eggSiteServicerPollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame],
                       pollingExchanger: PollingExchanger)
  extends StreamObserver[Proxy.PollingFrame] with Logging {
  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _
  private var inited = false

  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingReqSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    inited = true
    logDebug(s"PushPollingReqSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingReqSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")

    req.getSeq match {
      case 0L =>
        var shouldStop = false
        var batch: Proxy.PollingFrame = null

        while (!shouldStop) {
          batch = pollingExchanger.respQ.take()
          ensureInited(batch)

          if (batch.getMethod.equals(PollingMethods.FINISH_PUSH)) {
            shouldStop = true
          }
          eggSiteServicerPollingRespSO.onNext(batch)
        }
      case 1L =>
        pollingExchanger.reqQ.put(Proxy.PollingFrame.newBuilder().setMetadata(req.getMetadata).build())
      case _ =>
        val t: Throwable = new IllegalStateException(s"PushPollingReqSO.error: invalid seq=${req.getSeq} for rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
        onError(t)
    }
    logTrace(s"PushPollingReqSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PushPollingReqSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    eggSiteServicerPollingRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"PushPollingReqSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PushPollingReqSO.onCompleted calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    eggSiteServicerPollingRespSO.onCompleted()
    logTrace(s"PushPollingReqSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}



// polling client side
class DispatchPollingRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _

  private val self = this

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    method = req.getMethod

    method match {
      case "push" =>
        metadata = req.getPacket.getHeader
        delegateSO = new PushPollingRespSO(pollingResults)
      case "unary_call" =>
        metadata = req.getPacket.getHeader
        delegateSO = new UnaryCallPollingRespSO(pollingResults)
      case _ =>
        val t = new NotImplementedError(s"operation ${method} not supported")
        logError("fail to dispatch response", t)
        onError(TransferExceptionUtils.throwableToException(t))
        throw t
    }

    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)
    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    ensureInit(req)

    delegateSO.onNext(req)
  }

  override def onError(t: Throwable): Unit = {
    if (delegateSO != null) {
      delegateSO.onError(t)
    }
    LongPollingClient.releaseSemaphore()
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
    LongPollingClient.releaseSemaphore()
  }
}


class PushPollingRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private var putBatchSinkPushReqSO: StreamObserver[Proxy.Packet] = _
  private var putBatchPollingPushRespSO: StreamObserver[Proxy.Metadata] = _

  private val self = this

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingRespSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    putBatchPollingPushRespSO = new PutBatchPollingPushRespSO(pollingResults)
    putBatchSinkPushReqSO = new PutBatchSinkPushReqSO(putBatchPollingPushRespSO)

    inited = true
    logDebug(s"PushPollingRespSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingRespSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    ensureInit(req)

    if (req.getMethod != PollingMethods.FINISH_PUSH) {
      putBatchSinkPushReqSO.onNext(req.getPacket)
    } else {
      putBatchSinkPushReqSO.onCompleted()
    }
    logTrace(s"PushPollingRespSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PushPollingRespSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    putBatchSinkPushReqSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"PushPollingRespSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PushPollingRespSO.onComplete calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
//    nextReqSO.onCompleted()
    logTrace(s"PushPollingRespSO.onComplete called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

/**
 * Polling rs gets put batch resp and pass
 */
class PutBatchPollingPushRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private var pollingFrameSeq = 0

  private def ensureInited(req: Proxy.Metadata): Unit = {
    logTrace(s"PutBatchPollingPushRespSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logDebug(s"PutBatchPollingPushRespSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(resp: Proxy.Metadata): Unit = {
    logTrace(s"PutBatchPollingPushRespSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    ensureInited(resp)
    pollingFrameSeq += 1
    val respPollingFrame = Proxy.PollingFrame.newBuilder()
      .setMethod(PollingMethods.PUSH)
      .setMetadata(resp)
      .setSeq(pollingFrameSeq)
      .build()

    pollingResults.put(respPollingFrame)
    logTrace(s"PutBatchPollingPushRespSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PutBatchPollingPushRespSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    pollingResults.setError(TransferExceptionUtils.throwableToException(t))
    logError(s"PutBatchPollingPushRespSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PutBatchPollingPushRespSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    pollingResults.setFinish()
    logTrace(s"PutBatchPollingPushRespSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

// used by polling client
class UnaryCallPollingRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private val self = this
  private var stub: DataTransferServiceGrpc.DataTransferServiceBlockingStub = _

  private var pollingFrameSeq = 0

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingRespSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    val endpoint = Router.query(metadata.getDst.getPartyId, metadata.getDst.getRole).point
    val channel = GrpcClientUtils.getChannel(endpoint)
    stub = DataTransferServiceGrpc.newBlockingStub(channel)

    inited = true
    logDebug(s"UnaryCallPollingRespSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingRespSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    ensureInit(req)

    val callResult = stub.unaryCall(req.getPacket)

    pollingFrameSeq += 1
    val response = Proxy.PollingFrame.newBuilder()
      .setMethod(PollingMethods.UNARY_CALL)
      .setPacket(callResult)
      .setSeq(pollingFrameSeq)
      .build()

    pollingResults.put(response)
    pollingResults.setFinish()

    logTrace(s"UnaryCallPollingRespSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"UnaryCallPollingRespSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    pollingResults.setError(TransferExceptionUtils.throwableToException(t))
    logError(s"UnaryCallPollingRespSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"UnaryCallPollingRespSO.onCompleted calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    pollingResults.setFinish()
    logTrace(s"UnaryCallPollingRespSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}


class MockPollingReqSO(pollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _
  //  private def ensureInited(req: Proxy.PollingFrame): Unit = {
  //    if (inited) return
  //
  //    val dstPartyId = req.getMetadata.getDst.getPartyId
  //    val method = req.getMethod
  //
  //    method match {
  //      case "push" =>
  //        delegateSO = new PushPollingReqSO(pollingRespSO)
  //        PollingHelper.putPushPollingReqSO(dstPartyId, delegateSO.asInstanceOf[PushPollingReqSO])
  //      case "unaryCall" =>
  //        delegateSO = new UnaryCallPollingReqSO(pollingRespSO)
  //        PollingHelper.putUnaryCallPollingReqSO(dstPartyId, delegateSO.asInstanceOf[UnaryCallPollingReqSO])
  //      case _ =>
  //        val e = new NotImplementedError(s"method ${method} not supported")
  //        onError(e)
  //    }
  //
  //    inited = true
  //  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logDebug(s"onNext.$req")
    pollingRespSO.onNext(Proxy.PollingFrame.newBuilder().setMethod("push").setSeq(12399l).build())

    //    ensureInited(req)
    //    delegateSO.onNext(req)
  }

  override def onError(t: Throwable): Unit = {
    logError("DelegatePollingReqSO.onError", t)
    //    delegateSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    //    delegateSO.onCompleted()
    logDebug("DelegatePollingReqSO.onComplete")
    pollingRespSO.onCompleted()
  }
}

class MockPollingRespSO(pollingResults: PollingResults) extends StreamObserver[Proxy.PollingFrame] with Logging {
  override def onNext(v: Proxy.PollingFrame): Unit = {
    logInfo(s"onNext:$v")
    pollingResults.put(v)
  }

  override def onError(throwable: Throwable): Unit = {
    logError(throwable)
  }

  override def onCompleted(): Unit = {
    logInfo("complete")
  }
}