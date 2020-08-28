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

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, Semaphore, TimeUnit}

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
      val dispatchPollingRespSO = new DispatchPollingRespSO()
      val pollingReqSO = stub.polling(dispatchPollingRespSO)

      dispatchPollingRespSO.setPollingReqSo(pollingReqSO)
      pollingReqSO.onNext(
        LongPollingClient.initPollingFrameBuilder
          .setMethod(method).build())

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
  private val pushPollingSOs: LoadingCache[String, LinkedBlockingQueue[PushPollingReqSO]] = CacheBuilder.newBuilder
    .maximumSize(100000)
    // TODO:0: configurable
    .expireAfterAccess(1, TimeUnit.HOURS)
    .concurrencyLevel(50)
    .recordStats
    .build(new CacheLoader[String, LinkedBlockingQueue[PushPollingReqSO]]() {
      override def load(key: String): LinkedBlockingQueue[PushPollingReqSO] = {
        new LinkedBlockingQueue[PushPollingReqSO]()
      }
    })

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

  def getPushPollingReqSO(partyId: String, timeout: Long, unit: TimeUnit): PushPollingReqSO = {
    var result: PushPollingReqSO = null
    while (result == null) {
      // TODO:0: configurable
      result = pushPollingSOs.get(partyId).poll(timeout, unit)

      if (result.pushPollingRespSO.isCancelled) result == null
    }

    result
  }

  def getUnaryCallPollingReqSO(partyId: String, timeout: Long, unit: TimeUnit): UnaryCallPollingReqSO = {
    var result: UnaryCallPollingReqSO = null
    while (result == null) {
      // TODO:0: configurable
      result = unaryCallPollingSOs.get(partyId).poll(timeout, unit)

      if (result.respSO.isCancelled) result == null
    }

    result
  }

  def putPushPollingReqSO(partyId: String, reqSO: PushPollingReqSO): Unit = {
    pushPollingSOs.get(partyId).put(reqSO)
  }

  def putUnaryCallPollingReqSO(partyId: String, reqSO: UnaryCallPollingReqSO): Unit = {
    unaryCallPollingSOs.get(partyId).put(reqSO)
  }

  def isPartyIdPollingPush(partyId: String): Boolean = {
    pushPollingSOs.asMap().containsKey(partyId)
  }

  def isPartyIdPollingUnaryCall(partyId: String): Boolean = {
    unaryCallPollingSOs.asMap().containsKey(partyId)
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
 * @param respSO
 */
class DispatchPollingReqSO(respSO: ServerCallStreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _
  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    val dstPartyId = req.getMetadata.getDst.getPartyId
    val method = req.getMethod

    method match {
      case "push" =>
        delegateSO = new PushPollingReqSO(respSO)
        PollingHelper.putPushPollingReqSO(dstPartyId, delegateSO.asInstanceOf[PushPollingReqSO])
      case "unaryCall" =>
        delegateSO = new UnaryCallPollingReqSO(respSO)
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
    logError("DelegatePollingReqSO.onError", t)
    delegateSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
    logDebug("DelegatePollingReqSO.onComplete")
  }
}


class UnaryCallPollingReqSO(val respSO: ServerCallStreamObserver[Proxy.PollingFrame])
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

  var pushRespSO: StreamObserver[Proxy.Metadata] = _

  def setPushRespSO(prevRespSO: StreamObserver[Proxy.Metadata]): Unit = {
    this.pushRespSO = prevRespSO
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logWarning("debug1234")
    if (req.getSeq > 0) {
      pushRespSO.onNext(req.getMetadata)
    }
  }

  override def onError(t: Throwable): Unit = {
    logError("PushPollingReqSO.onError", t)
    pushRespSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("PushPollingReqSO.onComplete")
    pushRespSO.onCompleted()
  }
}



// polling client side
class DispatchPollingRespSO()
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _

  private val self = this

  private var pollingReqSO: StreamObserver[Proxy.PollingFrame] = _

  def setPollingReqSo(pollingReqSO: StreamObserver[Proxy.PollingFrame]): Unit = {
    this.pollingReqSO = pollingReqSO
  }

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    method = req.getMethod

    method match {
      case "push" =>
        metadata = req.getPacket.getHeader
        delegateSO = new PushPollingRespSO(pollingReqSO)
      case "unaryCall" =>
        metadata = req.getMetadata
        delegateSO = new UnaryCallPollingRespSO(pollingReqSO)
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

    // TODO:0: configurable
    // TODO:1: unify with client
    Thread.sleep(1000)
    LongPollingClient.releaseSemaphore(method)
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
    LongPollingClient.releaseSemaphore(method)
  }
}


class PushPollingRespSO(pollingReqSO: StreamObserver[Proxy.PollingFrame])
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
    if (inited) return

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    nextRespSO = new PollingPutBatchPushRespSO(pollingReqSO)
    nextReqSO = new PutBatchSinkPushReqSO(nextRespSO)

    inited = true
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    ensureInit(req)

    nextReqSO.onNext(req.getPacket)
  }

  override def onError(t: Throwable): Unit = {
    logError("PushPollingRespSO onError", t)
    nextReqSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("PushPollingRespSO onComplete")
    nextReqSO.onCompleted()
  }
}

/**
 * Polling rs gets put batch resp and pass
 */
class PollingPutBatchPushRespSO(pollingReqSO: StreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var pollingFrameSeq = 0

  override def onNext(resp: Proxy.Metadata): Unit = {
    pollingFrameSeq += 1
    val respPollingFrame = Proxy.PollingFrame.newBuilder()
      .setMethod("push")
      .setMetadata(resp)
      .setSeq(pollingFrameSeq)
      .build()

    pollingReqSO.onNext(respPollingFrame)
  }

  override def onError(t: Throwable): Unit = {
    logError("PollingPutBatchPushRespSO.onError", t)
    pollingReqSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("PollingPutBatchPushRespSO.onCompleted")
    pollingReqSO.onCompleted()
  }
}

// used by polling client
class UnaryCallPollingRespSO(pollingReqSO: StreamObserver[Proxy.PollingFrame])
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

    pollingReqSO.onNext(response)
  }

  override def onError(t: Throwable): Unit = {
    logError("UnaryCallPollingRespSO.onError", t)
    pollingReqSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("UnaryCallPollingRespSO.onComplete")
    pollingReqSO.onCompleted()
  }
}

