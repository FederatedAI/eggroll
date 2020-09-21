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
import com.webank.eggroll.core.meta.ErRollSiteHeader
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import com.webank.eggroll.rollsite.PollingResults.errorPoison
import io.grpc.ConnectivityState
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.commons.lang3.StringUtils

import scala.concurrent.TimeoutException


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
      val finishLatch = new CountDownLatch(1)
      val pollingResults = new PollingResults()
      val dispatchPollingRespSO = new DispatchPollingRespSO(pollingResults, finishLatch)
      //val dispatchPollingRespSO = new MockPollingRespSO(pollingResults)
      var connectStat = channel.getState(true)

      // waiting for connection ready.
      var timeout = 300
      while (connectStat != ConnectivityState.READY && timeout > 0) {
        connectStat = channel.getState(true)
        logTrace(f"[POLLING][CLIENT] waiting for connection ready, connectStat is $connectStat")
        Thread.sleep(1000)
        timeout -= 1
      }

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
              // TODO:0: decide whether to remove this because there is no timeout / deadline in polling
              if (!finishLatch.await(RollSiteConfKeys.EGGROLL_ROLLSITE_ONCOMPLETED_WAIT_TIMEOUT.get().toLong, TimeUnit.SECONDS)) {
                throw new TimeoutException(s"longPollingClient.onCompleted latch timeout")
              }
              finished = true
            case PollingMethods.NO_DATA_POISON =>
              logDebug("polling timeout with no data")
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

      // TODO:0: configurable
    } catch {
      case t: Throwable =>
        logError("polling failed", t)
        Thread.sleep(1000)
    }
  }

  def pollingForever(): Unit = {
    while (true) {
      try {
        polling()
      } catch {
        case e: Throwable =>
          logError("polling failed", e)
      }
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
    var result: Proxy.PollingFrame = q.poll(5, TimeUnit.MINUTES)

    if (result == null) {
      result = PollingResults.noDataPoison
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
    logTrace(s"DispatchPollingReqSO.ensureInited calling.")
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
    logTrace(s"DispatchPollingReqSO.ensureInited called.")
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
  private var rsHeader: ErRollSiteHeader = _
  private var inited = false

  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logDebug(s"UnaryCallPollingReqSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingReqSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      var batch: Proxy.PollingFrame = null
      req.getSeq match {
        case 0L =>
          logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=0L starting")
          var i = 0
          while (batch == null) {
            batch = pollingExchanger.respQ.poll(1, TimeUnit.SECONDS)
            logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=0L, getting from pollingExchanger, i=${i}")
            i += 1
          }
          ensureInited(batch)

          eggSiteServicerPollingRespSO.onNext(batch)
          logTrace(s"UnaryCallPollingReqSO.onNext.req.getSeq=0L finished")
        case 1L =>
          logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=1L starting")
          pollingExchanger.reqQ.put(Proxy.PollingFrame.newBuilder().setPacket(req.getPacket).build())
          logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=1L finished")
        case _ =>
          val t: Throwable = new IllegalStateException(s"invalid seq=${req.getSeq} for rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          onError(t)
      }
    } catch {
      case t:Throwable =>
        onError(t)
    }

    logTrace(s"UnaryCallPollingReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"UnaryCallPollingReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)
    eggSiteServicerPollingRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"UnaryCallPollingReqSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"UnaryCallPollingReqSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    eggSiteServicerPollingRespSO.onCompleted()
    logTrace(s"UnaryCallPollingReqSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}

// server side. processes push polling req
class PushPollingReqSO(val eggSiteServicerPollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame],
                       pollingExchanger: PollingExchanger)
  extends StreamObserver[Proxy.PollingFrame] with Logging {
  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _
  private var inited = false

  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logDebug(s"PushPollingReqSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingReqSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      req.getSeq match {
        case 0L =>
          logTrace(s"PushPollingReqSO.onNext req.getSeq=0L starting")
          var shouldStop = false
          var batch: Proxy.PollingFrame = null

          while (!shouldStop) {
            var i = 0
            batch = null
            while (batch == null) {
              batch = pollingExchanger.respQ.poll(1, TimeUnit.SECONDS)
              logTrace(s"PushPollingReqSO.onNext req.getSeq=0L, getting from pollingExchanger, i=${i}")
              i += 1
            }
            ensureInited(batch)

            if (batch.getMethod.equals(PollingMethods.FINISH_PUSH)) {
              shouldStop = true
            }
            eggSiteServicerPollingRespSO.onNext(batch)
          }
          logTrace(s"PushPollingReqSO.onNext req.getSeq=0L finished")
        case 1L =>
          logTrace(s"PushPollingReqSO.onNext req.getSeq=1L starting")
          pollingExchanger.reqQ.put(Proxy.PollingFrame.newBuilder().setMetadata(req.getMetadata).build())
          logTrace(s"PushPollingReqSO.onNext req.getSeq=1L finished")
        case _ =>
          val t: Throwable = new IllegalStateException(s"PushPollingReqSO.error: invalid seq=${req.getSeq} for rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          onError(t)
      }
    } catch {
      case t:Throwable =>
        onError(t)
    }
    logTrace(s"PushPollingReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PushPollingReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)
    eggSiteServicerPollingRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"PushPollingReqSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PushPollingReqSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    eggSiteServicerPollingRespSO.onCompleted()
    logTrace(s"PushPollingReqSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}


// polling client side
class DispatchPollingRespSO(pollingResults: PollingResults,
                            finishLatch: CountDownLatch)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _
  private var method: String = _
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _

  private val self = this

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    logTrace(s"DispatchPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
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
    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logTrace(s"DispatchPollingRespSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"DispatchPollingRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    try {
      if (TransferExceptionUtils.checkPacketIsException(req.getPacket)) {
        val errStack = req.getPacket.getBody.getValue.toStringUtf8
        logError(s"DispatchPollingRespSO get error from push or unarycall. ${errStack}")
        onError(new Exception(f"DispatchPollingRespSO get error from push or unarycall, ${errStack}"))
      }

      ensureInit(req)
      delegateSO.onNext(req)
    } catch {
      case t:Throwable =>
        onError(t)
    }
    logTrace(s"DispatchPollingRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logTrace(s"DispatchPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (delegateSO != null) {
      delegateSO.onError(TransferExceptionUtils.throwableToException(t))
    }
    finishLatch.countDown()
    LongPollingClient.releaseSemaphore()
    logTrace(s"DispatchPollingRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"DispatchPollingRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    delegateSO.onCompleted()
    finishLatch.countDown()
    LongPollingClient.releaseSemaphore()
    logTrace(s"DispatchPollingRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}


class PushPollingRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _
  private var method: String = _

  private var putBatchSinkPushReqSO: StreamObserver[Proxy.Packet] = _
  private var putBatchPollingPushRespSO: StreamObserver[Proxy.Metadata] = _

  private val self = this

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    putBatchPollingPushRespSO = new PutBatchPollingPushRespSO(pollingResults)
    putBatchSinkPushReqSO = new PutBatchSinkPushReqSO(putBatchPollingPushRespSO)

    inited = true
    logDebug(s"PushPollingRespSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      ensureInit(req)

      if (req.getMethod != PollingMethods.FINISH_PUSH) {
        putBatchSinkPushReqSO.onNext(req.getPacket)
      } else {
        putBatchSinkPushReqSO.onCompleted()
      }
    } catch {
      case t:Throwable =>
        onError(t)
    }

    logTrace(s"PushPollingRespSO.onNext end. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PushPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)
    putBatchSinkPushReqSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"PushPollingRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PushPollingRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
//    nextReqSO.onCompleted()
    logTrace(s"PushPollingRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
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
  private var rsHeader: ErRollSiteHeader = _
  private var method: String = _

  private var pollingFrameSeq = 0

  private def ensureInited(req: Proxy.Metadata): Unit = {
    logTrace(s"PutBatchPollingPushRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logDebug(s"PutBatchPollingPushRespSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(resp: Proxy.Metadata): Unit = {
    logTrace(s"PutBatchPollingPushRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      ensureInited(resp)
      pollingFrameSeq += 1
      val respPollingFrame = Proxy.PollingFrame.newBuilder()
        .setMethod(PollingMethods.PUSH)
        .setMetadata(resp)
        .setSeq(pollingFrameSeq)
        .build()

      pollingResults.put(respPollingFrame)
    } catch {
      case t:Throwable =>
        onError(t)
    }

    logTrace(s"PutBatchPollingPushRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PutBatchPollingPushRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)
    pollingResults.setError(TransferExceptionUtils.throwableToException(t))
    logError(s"PutBatchPollingPushRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PutBatchPollingPushRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    pollingResults.setFinish()
    logTrace(s"PutBatchPollingPushRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}

// used by polling client
class UnaryCallPollingRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _
  private var method: String = _

  private val self = this
  private var stub: DataTransferServiceGrpc.DataTransferServiceBlockingStub = _

  private var pollingFrameSeq = 0

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    val endpoint = Router.query(metadata.getDst.getPartyId, metadata.getDst.getRole).point
    val channel = GrpcClientUtils.getChannel(endpoint)
    stub = DataTransferServiceGrpc.newBlockingStub(channel)

    inited = true
    logDebug(s"UnaryCallPollingRespSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"UnaryCallPollingRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      ensureInit(req)
      logTrace(s"UnaryCallPollingRespSO.onNext calling. do stub.unaryCall starting")
      val callResult = stub.unaryCall(req.getPacket)
      logTrace(s"UnaryCallPollingRespSO.onNext calling. do stub.unaryCall finished")
      pollingFrameSeq += 1
      val response = Proxy.PollingFrame.newBuilder()
        .setMethod(PollingMethods.UNARY_CALL)
        .setPacket(callResult)
        .setSeq(pollingFrameSeq)
        .build()
      logTrace(s"UnaryCallPollingRespSO.onNext calling. do pollingResults.put.")
      pollingResults.put(response)
      pollingResults.setFinish()
    } catch {
      case t:Throwable =>
        onError(t)
    }

    logTrace(s"UnaryCallPollingRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"UnaryCallPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)
    pollingResults.setError(TransferExceptionUtils.throwableToException(t))
    logError(s"UnaryCallPollingRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"UnaryCallPollingRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    pollingResults.setFinish()
    logTrace(s"UnaryCallPollingRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
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