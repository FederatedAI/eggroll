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

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.util.{Logging, ToStringUtils}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}


object LongPollingClient {
  private val defaultPollingReqMetadata: Proxy.Metadata = Proxy.Metadata.newBuilder()
    .setDst(
      Proxy.Topic.newBuilder()
        .setPartyId(RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()))
    .build()

  val initPollingFrameBuilder: Proxy.PollingFrame.Builder = Proxy.PollingFrame.newBuilder().setMetadata(defaultPollingReqMetadata)

  private val pollingSemaphore = new Semaphore(RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_PUSH_CONCURRENCY.get().toInt + RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_UNARYCALL_CONCURRENCY.get().toInt)

  def acquireSemaphore(method: String): Unit = {
    LongPollingClient.pollingSemaphore.acquire()
  }

  def releaseSemaphore(method: String): Unit = {
    LongPollingClient.pollingSemaphore.release()
  }
}

class LongPollingClient extends Logging {
  def polling(method: String): Unit = {
    LongPollingClient.acquireSemaphore(method)

    try {
      val endpoint = Router.query("default")
      val channel = GrpcClientUtils.getChannel(endpoint)
      val stub = DataTransferServiceGrpc.newStub(channel)
      val pollingResults = new PollingResults()
      val dispatchPollingRespSO = new DispatchPollingRespSO(pollingResults)
      //val dispatchPollingRespSO = new MockPollingRespSO(pollingResults)
      val pollingReqSO = stub.polling(dispatchPollingRespSO)

      pollingReqSO.onNext(
        LongPollingClient.initPollingFrameBuilder
          .setMethod(method).build())

      try {
        for (req <- pollingResults) {
          if (req != null) {
            logInfo(s"client onNext. req=${ToStringUtils.toOneLineString(req)}")
            pollingReqSO.onNext(req)
          }
        }
      } catch {
        case t: Throwable =>
          pollingReqSO.onError(TransferExceptionUtils.throwableToException(t))
      }

      pollingReqSO.onCompleted()
      pollingResults.await()


      // TODO:0: configurable
    } catch {
      case t: Throwable =>
        logError("polling failed", t)
        Thread.sleep(1000)
    }
  }

  def pollingForever(method: String): Unit = {
    while (true) {
      polling(method)
    }
  }
}

object PollingHelper {
  private val unaryCallPollingSOs: LoadingCache[String, LinkedBlockingQueue[UnaryCallPollingReqSO]] = CacheBuilder.newBuilder
    .maximumSize(100000)
    // TODO:0: configurable
    .expireAfterAccess(1, TimeUnit.HOURS)
    .concurrencyLevel(50)
    .recordStats
    .build(new CacheLoader[String, LinkedBlockingQueue[UnaryCallPollingReqSO]]() {
      override def load(key: String): LinkedBlockingQueue[UnaryCallPollingReqSO] = {
        new LinkedBlockingQueue[UnaryCallPollingReqSO]()
      }
    })

  def getUnaryCallPollingReqSO(partyId: String, timeout: Long, unit: TimeUnit): UnaryCallPollingReqSO = {
    var result: UnaryCallPollingReqSO = null
    while (result == null) {
      // TODO:0: configurable
      result = unaryCallPollingSOs.get(partyId).poll(timeout, unit)

      if (result.pollingRespSO.isCancelled) result == null
    }

    result
  }

  def putUnaryCallPollingReqSO(partyId: String, reqSO: UnaryCallPollingReqSO): Unit = {
    unaryCallPollingSOs.get(partyId).put(reqSO)
  }

  def isPartyIdPollingUnaryCall(partyId: String): Boolean = {
    unaryCallPollingSOs.asMap().containsKey(partyId)
  }

  val pollingReqQueue = new SynchronousQueue[Proxy.PollingFrame]()
  val pollingRespQueue = new SynchronousQueue[Proxy.PollingFrame]()

}

class PollingResults() extends Iterator[Proxy.PollingFrame] with Logging {
  private val q = new LinkedBlockingQueue[Proxy.PollingFrame]()
  private val error: AtomicReference[Throwable] = new AtomicReference[Throwable](null)
  private val finishLatch = new CountDownLatch(1)

  def put(f: Proxy.PollingFrame): Unit = {
    q.put(f)
  }

  def isFinished: Boolean = finishLatch.getCount == 0

  def raise(t: Throwable): Unit = this.error.compareAndSet(null, t)

  def countdown(): Unit = finishLatch.countDown()

  def await(): Unit = finishLatch.await()

  override def hasNext: Boolean = {
    val e = error.get()
    if (e != null) throw e

    !(q.isEmpty && isFinished)
  }

  override def next(): Proxy.PollingFrame = {
    var result: Proxy.PollingFrame = null
    while (hasNext) {
      result = q.poll(1, TimeUnit.SECONDS)

      if (result != null) {
        logWarning(s"polled result: ${ToStringUtils.toOneLineString(result)}")
        return result
      }
    }

    result
  }
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
 * @param pollingRespSO
 */
class DispatchPollingReqSO(pollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _
  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    val dstPartyId = req.getMetadata.getDst.getPartyId
    val method = req.getMethod

    method match {
      case "push" =>
        delegateSO = new PushPollingReqSO(pollingRespSO)
      case "unaryCall" =>
        delegateSO = new UnaryCallPollingReqSO(pollingRespSO)
        PollingHelper.putUnaryCallPollingReqSO(dstPartyId, delegateSO.asInstanceOf[UnaryCallPollingReqSO])
      case _ =>
        val e = new NotImplementedError(s"method ${method} not supported")
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


class UnaryCallPollingReqSO(val pollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var unaryCallRespSO: StreamObserver[Proxy.Packet] = _

  def setUnaryCallRespSO(prevRespSO: StreamObserver[Proxy.Packet]): Unit = {
    this.unaryCallRespSO = prevRespSO
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    if (req.getSeq > 0) {
      unaryCallRespSO.onNext(req.getPacket)
    }
  }

  override def onError(t: Throwable): Unit = {
    logError("UnaryCallPollingReqSO.onError", t)
    unaryCallRespSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("UnaryCallPollingReqSO.onComplete")
    unaryCallRespSO.onCompleted()
  }
}

// server side. processes push polling req
class PushPollingReqSO(val pushPollingRespSO: ServerCallStreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {
  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _
  private var inited = false

  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    logTrace(s"onInit calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    inited = true
    logDebug(s"onInit called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")

    if (req.getSeq == 0L) {
      var shouldStop = false
      var batch: Proxy.PollingFrame = null

      while (!shouldStop) {
        batch = PollingHelper.pollingRespQueue.take()
        ensureInited(batch)

        logWarning(s"debug 1234 PushPollingReqSO batch=${ToStringUtils.toOneLineString(batch)}")

        if (batch.getMethod.equals("finish_push")) {
          shouldStop = true
        }
        pushPollingRespSO.onNext(batch)
      }
    } else {
      PollingHelper.pollingReqQueue.put(Proxy.PollingFrame.newBuilder().setMetadata(req.getMetadata).build())
    }
    logTrace(s"onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    pushPollingRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    //PollingHelper.pollingReqQueue.put(Proxy.PollingFrame.newBuilder().setMetadata(metadata).build())
    pushPollingRespSO.onCompleted()
    logTrace(s"onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
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
      case "unaryCall" =>
        metadata = req.getMetadata
        delegateSO = new UnaryCallPollingRespSO(pollingResults)
      case _ =>
        val t = new NotImplementedError(s"operation ${method} not supported")
        onError(TransferExceptionUtils.throwableToException(t))
        throw t
    }

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
    LongPollingClient.releaseSemaphore(method)
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
    LongPollingClient.releaseSemaphore(method)
  }
}


class PushPollingRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private var nextReqSO: StreamObserver[Proxy.Packet] = _
  private var nextRespSO: StreamObserver[Proxy.Metadata] = _

  private val self = this

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    logTrace(s"onInit calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    nextRespSO = new PollingPutBatchPushRespSO(pollingResults)
    nextReqSO = new PutBatchSinkPushReqSO(nextRespSO)

    inited = true
    logDebug(s"onInit called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    ensureInit(req)

    if (req.getMethod != "finish_push") {
      nextReqSO.onNext(req.getPacket)
    } else {
      nextReqSO.onCompleted()
    }
    logTrace(s"onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    nextReqSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"onComplete calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
//    nextReqSO.onCompleted()
    logTrace(s"onComplete called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

/**
 * Polling rs gets put batch resp and pass
 */
class PollingPutBatchPushRespSO(pollingResults: PollingResults)
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _

  private var pollingFrameSeq = 0

  private def ensureInited(req: Proxy.Metadata): Unit = {
    logTrace(s"onInit calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = req
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logDebug(s"onInit called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(resp: Proxy.Metadata): Unit = {
    logTrace(s"onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    pollingFrameSeq += 1
    val respPollingFrame = Proxy.PollingFrame.newBuilder()
      .setMethod("push")
      .setMetadata(resp)
      .setSeq(pollingFrameSeq)
      .build()

    pollingResults.put(respPollingFrame)
    logTrace(s"onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    pollingResults.raise(TransferExceptionUtils.throwableToException(t))
    pollingResults.countdown()
    logError(s"onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    pollingResults.countdown()
    logTrace(s"onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
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
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    val endpoint = Router.query(rsHeader.dstPartyId, rsHeader.dstRole)
    val channel = GrpcClientUtils.getChannel(endpoint)
    stub = DataTransferServiceGrpc.newBlockingStub(channel)

    inited = true
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    ensureInit(req)

    val callResult = stub.unaryCall(req.getPacket)

    pollingFrameSeq += 1
    val response = Proxy.PollingFrame.newBuilder()
      .setMethod("unaryCall")
      .setPacket(callResult)
      .setSeq(pollingFrameSeq)
      .build()

    pollingResults.put(response)
  }

  override def onError(t: Throwable): Unit = {
    logError("UnaryCallPollingRespSO.onError", t)
    pollingResults.raise(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("UnaryCallPollingRespSO.onComplete")
    pollingResults.countdown()
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