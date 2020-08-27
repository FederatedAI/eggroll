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

  val initPollingFrame: Proxy.PollingFrame = Proxy.PollingFrame.newBuilder().setMetadata(defaultPollingReqMetadata).build()

  val pollingConcurrencySemaphore = new Semaphore(RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_PULL_CONCURRENCY.get().toInt)
}

class LongPollingClient extends Logging {
  def polling(): Unit = {
    LongPollingClient.pollingConcurrencySemaphore.acquire()

    try {
      val endpoint = Router.query("default")
      val channel = GrpcClientUtils.getChannel(endpoint)
      val stub = DataTransferServiceGrpc.newStub(channel)
      val finishLatch = new CountDownLatch(1)
      val dispatchPollingRespSO = new DispatchPollingRespSO(finishLatch)
      val pollingReqSO = stub.polling(dispatchPollingRespSO)
      dispatchPollingRespSO.setPollingReqSo(pollingReqSO)
      pollingReqSO.onNext(LongPollingClient.initPollingFrame)

      // TODO:0: configurable
      //finishLatch.await(1, TimeUnit.HOURS)
    } catch {
      case _ =>
        Thread.sleep(1000)
    }
  }

  def pollingDaemon(): Unit = {
    while (true) {
      polling()
    }
  }
}

object PollingHelper {
  val pollingSOs: LoadingCache[String, LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]]] = CacheBuilder.newBuilder
    .maximumSize(100000)
    // TODO:0: configurable
    .expireAfterAccess(1, TimeUnit.HOURS)
    .concurrencyLevel(50)
    .recordStats
    .build(new CacheLoader[String, LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]]]() {
      override def load(key: String): LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]] = {
        new LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]]()
      }
    })
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
 * @param nextRespSO
 */
class PollingReqSO(nextRespSO: StreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false
  private def ensureInited(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    val dstPartyId = req.getMetadata.getDst.getPartyId
    PollingHelper.pollingSOs.get(dstPartyId).put(nextRespSO.asInstanceOf[ServerCallStreamObserver[Proxy.PollingFrame]])

    inited = true
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    ensureInited(req)
  }

  override def onError(t: Throwable): Unit = {
    logError("DelegatePollingReqSO.onError", t)
    nextRespSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("DelegatePollingReqSO.onComplete")
  }
}

class DispatchPollingRespSO(finishLatch: CountDownLatch)
  extends StreamObserver[Proxy.PollingFrame] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var method: String = _
  private var delegateSO: StreamObserver[Proxy.PollingFrame] = _

  private val self = this

  private var pollingReqSO: StreamObserver[Proxy.PollingFrame] = _

  def setPollingReqSo(pollingReqSO: StreamObserver[Proxy.PollingFrame]): Unit = this.pollingReqSO = pollingReqSO

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    if (inited) return

    method = req.getMethod

    method match {
      case "push" =>
        metadata = req.getPacket.getHeader
        delegateSO = new PushPollingRespSO(pollingReqSO)
      case "unaryCall" =>
        metadata = req.getMetadata
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
    LongPollingClient.pollingConcurrencySemaphore.release()
    finishLatch.countDown()
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
    LongPollingClient.pollingConcurrencySemaphore.release()
    finishLatch.countDown()
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
    nextReqSO.onCompleted()
  }

  override def onCompleted(): Unit = {
    logDebug("PushPollingRespSO onComplete")
    nextReqSO.onCompleted()
  }
}

/**
 * Polling rs gets put batch resp and save
 */
class PollingPutBatchPushRespSO(pollingReqSO: StreamObserver[Proxy.PollingFrame])
  extends StreamObserver[Proxy.Metadata] with Logging {

  override def onNext(resp: Proxy.Metadata): Unit = {
    val respPollingFrame = Proxy.PollingFrame.newBuilder().setMethod("push").setMetadata(resp).build()
    pollingReqSO.onNext(respPollingFrame)
  }

  override def onError(t: Throwable): Unit = {
    logError("PollingPutBatchPushRespSO.onError", t)
    pollingReqSO.onError(TransferExceptionUtils.throwableToException(t))
  }

  override def onCompleted(): Unit = {
    logDebug("ignoring PollingPutBatchPushRespSO.onCompleted")
    pollingReqSO.onCompleted()
  }
}


class PollingUnaryCallRespSO()
  extends StreamObserver[Proxy.PollingFrame] with Logging {
  override def onNext(req: Proxy.PollingFrame): Unit = ???

  override def onError(t: Throwable): Unit = ???

  override def onCompleted(): Unit = ???
}

