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

import java.util.concurrent.{CountDownLatch, Future, TimeUnit, TimeoutException}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.meta.{ErJob, ErRollSiteHeader, ErTask}
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.transfer.{GrpcClientUtils, Transfer, TransferServiceGrpc}
import com.webank.eggroll.core.util.{IdUtils, Logging, ToStringUtils}
import com.webank.eggroll.rollpair.{RollPair, RollPairContext}
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils

import scala.collection.parallel.mutable


class DispatchPushReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var delegateSO: StreamObserver[Proxy.Packet] = _
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _

  private def ensureInited(req: Proxy.Packet): Unit = {
    if (inited) return

    metadata = req.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val encodedRollSiteHeader = metadata.getExt
    rsHeader = RollSiteHeader.parseFrom(encodedRollSiteHeader).fromProto()
    rsKey = rsHeader.getRsKey()

    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
    val dstPartyId = req.getHeader.getDst.getPartyId
    val dstRole = req.getHeader.getDst.getRole
    val logMsg = s"DispatchPushReqSO.ensureInited. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}"
    delegateSO =  if (myPartyId.equals(dstPartyId)) {
      if (isLogTraceEnabled()) {
        logTrace(s"${logMsg}, hop=SINK")
      }
      new PutBatchSinkPushReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO)
    } else {
      val dstIsPolling = Router.query(dstPartyId, dstRole).isPolling
      if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_SERVER_ENABLED.get().toBoolean && dstIsPolling) {
      // polling mode
        logTrace(s"${logMsg}, hop=FORWARD, type=PUSH2POLLING")
        new ForwardPushToPollingReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO)
      } else {
        logTrace(s"${logMsg}, hop=FORWARD, type=PUSH2PUSH")
        new ForwardPushReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO)
      }
    }
    inited = true
  }

  override def onNext(request: Proxy.Packet): Unit = {
    ensureInited(request)

    delegateSO.onNext(request)
  }

  override def onError(throwable: Throwable): Unit = {
    delegateSO.onError(throwable)
  }

  override def onCompleted(): Unit = {
    delegateSO.onCompleted()
  }
}


object PutBatchSinkUtils {
  val sessionCache: LoadingCache[String, ErSession] = CacheBuilder.newBuilder
    .maximumSize(2000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .concurrencyLevel(100)
    .recordStats
    .softValues
    .build(new CacheLoader[String, ErSession]() {
      override def load(key: String): ErSession = {
        new ErSession(sessionId = key, createIfNotExists = false)
      }
    })
}


class PutBatchSinkPushReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false
  //private var ctx: RollPairContext = _

  // TODO:1: abstract the following 5 variables with other SOs
  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _

  private val finishLatch = new CountDownLatch(1)
  private val self = this
  private var putBatchSinkPushReqSO: StreamObserver[Transfer.TransferBatch] = _

  private var brokerTag: String = _

  private val transferBatchBuilder = Transfer.TransferBatch.newBuilder()
  private val transferHeaderBuilder = Transfer.TransferHeader.newBuilder()
    .setTotalSize(-1L)
    .setStatus("stream_partition")

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    if (inited) return
    logTrace(s"PutBatchSinkPushReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val encodedRollSiteHeader = metadata.getExt
    rsHeader = RollSiteHeader.parseFrom(encodedRollSiteHeader).fromProto()
    rsKey = rsHeader.getRsKey()

    val sessionId = String.join("_", rsHeader.rollSiteSessionId, rsHeader.dstRole, rsHeader.dstPartyId)
    val session = PutBatchSinkUtils.sessionCache.get(sessionId)

    if (!SessionStatus.ACTIVE.equals(session.sessionMeta.status)) {
      val error = new IllegalStateException(s"session=${sessionId} with illegal status. expected=${SessionStatus.ACTIVE}, actual=${session.sessionMeta.status}")
      onError(error)
      throw error
    }

    val namespace = rsHeader.rollSiteSessionId
    val name = rsKey
    val ctx = new RollPairContext(session)

    var rpOptions = rsHeader.options ++ Map(StringConstants.TOTAL_PARTITIONS_SNAKECASE -> rsHeader.totalPartitions.toString)
    if (rsHeader.dataType.equals("object")) {
      rpOptions ++= Map(StringConstants.SERDES -> SerdesTypes.EMPTY)
    } else {
      rpOptions ++= Map(StringConstants.SERDES -> rsHeader.options.getOrElse("serdes", SerdesTypes.PICKLE))
    }

    // table creates here
    val rp = ctx.load(namespace, name, options = rpOptions)

    val partitionId = rsHeader.partitionId
    val partition = rp.store.partitions(partitionId)
    val egg = ctx.session.routeToEgg(partition)

    brokerTag = s"${RollPair.PUT_BATCH}-${rsHeader.getRsKey()}-${partitionId}"

    val jobId = IdUtils.generateJobId(sessionId = ctx.session.sessionId, tag = brokerTag)
    val job = ErJob(
      id = jobId,
      name = RollPair.PUT_BATCH,
      inputs = Array(rp.store),
      outputs = Array(rp.store),
      functors = Array.empty,
      options = rsHeader.options ++ Map(SessionConfKeys.CONFKEY_SESSION_ID -> ctx.session.sessionId))

    val task = ErTask(id = brokerTag,
      name = RollPair.PUT_BATCH,
      inputs = Array(partition),
      outputs = Array(partition),
      job = job)

    val commandFuture: Future[ErTask] = RollPairContext.executor.submit(() => {
      val commandClient = new CommandClient(egg.commandEndpoint)
      commandClient.call[ErTask](RollPair.EGG_RUN_TASK_COMMAND, task)
    })

    val channel = GrpcClientUtils.getChannel(egg.transferEndpoint)
    val stub = TransferServiceGrpc.newStub(channel)

    putBatchSinkPushReqSO = stub.send(new PutBatchSinkPushRespSO(metadata, commandFuture, eggSiteServicerPushRespSO_forwardPushToPollingRespSO, finishLatch))

    inited = true
    logDebug(s"PutBatchSinkPushReqSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(request: Proxy.Packet): Unit = {
    logTrace(s"PutBatchSinkPushReqSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}, data_size=${request.getBody.getValue.size()}")
    ensureInited(request)

    try {
      if (TransferExceptionUtils.checkPacketIsException(request)) {
        val errStack = request.getBody.getValue.toStringUtf8
        logError(s"PutBatchSinkPushReqSO.onNext error received from prev. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}, stack=${errStack}")
        // todo:0: error handing
      } else {
        val tbHeader = transferHeaderBuilder.setId(metadata.getSeq.toInt)
          .setTag(brokerTag)
          .setExt(request.getHeader.getExt)

        val tbBatch = transferBatchBuilder.setHeader(tbHeader)
          .setData(request.getBody.getValue)
          .build()

        putBatchSinkPushReqSO.onNext(tbBatch)
      }
    } catch {
      case t: Throwable => {
        onError(t)
      }
    }
    logTrace(s"PutBatchSinkPushReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"PutBatchSinkPushReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onError(wrapped)
    logError(s"PutBatchSinkPushReqSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PutBatchSinkPushReqSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    putBatchSinkPushReqSO.onCompleted()
    if (!finishLatch.await(RollSiteConfKeys.EGGROLL_ROLLSITE_ONCOMPLETED_WAIT_TIMEOUT.get().toLong, TimeUnit.SECONDS)) {
      onError(new TimeoutException(s"PutBatchSinkPushReqSO.onCompleted latch timeout. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}"))
    }

    logTrace(s"PutBatchSinkPushReqSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}

class PutBatchSinkPushRespSO(val reqHeader: Proxy.Metadata,
                             val commandFuture: Future[ErTask],
                             val eggSiteServicerPushRespSO_putBatchPollingPushRespSO: StreamObserver[Proxy.Metadata],
                             val finishLatch: CountDownLatch)
  extends StreamObserver[Transfer.TransferBatch] with Logging {

  private var transferHeader: Transfer.TransferHeader = _
  private var oneLineStringTransferHeader: String = _

  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _

  override def onNext(resp: Transfer.TransferBatch): Unit = {
    logTrace(s"PutBatchSinkPushRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}")
    transferHeader = resp.getHeader
    oneLineStringTransferHeader = ToStringUtils.toOneLineString(transferHeader)
    rsHeader = RollSiteHeader.parseFrom(transferHeader.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    try {
      logTrace(s"PutBatchSinkPushRespSO.onNext calling, command completing. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}")
      commandFuture.get()
      logTrace(s"PutBatchSinkPushRespSO.onNext calling, command completed. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}")
      eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onNext(reqHeader.toBuilder.setAck(resp.getHeader.getId).build())
      eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onCompleted()
    } catch {
      case t: Throwable =>
        onError(t)
    }
    logDebug(s"PutBatchSinkPushRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"PutBatchSinkPushRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}", wrapped)
    eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onError(wrapped)
    finishLatch.countDown()
    logError(s"PutBatchSinkPushRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}", t)
  }

  override def onCompleted(): Unit = {
    logTrace(s"PutBatchSinkPushRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}")
    finishLatch.countDown()
    logTrace(s"PutBatchSinkPushRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, transferHeader=${oneLineStringTransferHeader}")
  }
}


class ForwardPushToPollingReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _

  private val self = this
  private var failRequest: mutable.ParHashSet[Proxy.Metadata] = new mutable.ParHashSet[Proxy.Metadata]()
  private val pollingFrameBuilder = Proxy.PollingFrame.newBuilder().setMethod("push")
  private var pollingFrameSeq = 0
  private var pollingExchanger: PollingExchanger = null

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    if (inited) return
    logTrace(s"ForwardPushToPollingReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    while (pollingExchanger == null) {
      val partyId = firstRequest.getHeader.getDst.getPartyId
      pollingExchanger = PollingExchanger.getPollingExchangerQueue(partyId).poll(
        RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_Q_POLL_INTERVAL_SEC.get().toLong, TimeUnit.SECONDS)
      logTrace(s"ForwardPushToPollingReqSO.ensureInited, polling from pollingExchanger. partyId=${partyId}, isNull=${pollingExchanger == null}, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    }

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    pollingExchanger.setMethod(PollingMethods.PUSH)
    inited = true
    logDebug(s"ForwardPushToPollingReqSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.Packet): Unit = {
    logTrace(s"ForwardPushToPollingReqSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      ensureInited(req)

      pollingFrameSeq += 1
      pollingFrameBuilder.setSeq(pollingFrameSeq).setPacket(req)
      val nextFrame = pollingFrameBuilder.build()

      PollingExchanger.offer(nextFrame, pollingExchanger.respQ,
        "ForwardPushToPollingReqSO.onNext, offering frame to pollingExchanger.respQ, ", rsHeader, oneLineStringMetadata)
    } catch {
      case t: Throwable =>
        val errorPacket = TransferExceptionUtils.genExceptionToNextSite(req, t)
        pollingFrameSeq += 1
        pollingFrameBuilder.setSeq(pollingFrameSeq).setPacket(errorPacket)
        val nextFrame = pollingFrameBuilder.build()

        pollingExchanger.respQ.clear()
        PollingExchanger.offer(nextFrame, pollingExchanger.respQ,
          "ForwardPushToPollingReqSO.onNext, offering error to pollingExchanger.respQ, ", rsHeader, oneLineStringMetadata)

        onError(t)
    }

    logTrace(s"ForwardPushToPollingReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"ForwardPushToPollingReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onError(wrapped)
    logError(s"ForwardPushToPollingReqSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPushToPollingReqSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    pollingFrameSeq += 1
    pollingFrameBuilder.setSeq(pollingFrameSeq).setMethod(PollingMethods.SUB_STREAM_FINISH)
    val subStreamFinishFrame = pollingFrameBuilder.build()

    try {
      PollingExchanger.offer(subStreamFinishFrame, pollingExchanger.respQ, "ForwardPushToPollingReqSO.onCompleted, offering to pollingExchanger.respQ, ", rsHeader, oneLineStringMetadata)

      var pollingReq: Proxy.PollingFrame = null

      pollingReq = PollingExchanger.poll(pollingExchanger.reqQ,
        "ForwardPushToPollingReqSO.onCompleted, getting from pollingExchanger.reqQ", rsHeader, oneLineStringMetadata)

      if (PollingMethods.ERROR_POISON.equals(pollingReq.getMethod)) {
        val t = TransferExceptionUtils.throwableToException(new RuntimeException(s"exception from polling: ${pollingReq.getDesc}"))
        onError(t)
        return
      }

      eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onNext(pollingReq.getMetadata)
      eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onCompleted()
    } catch {
      case t: Throwable =>
        onError(t)
    }

    logTrace(s"ForwardPushToPollingReqSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}


class ForwardPushReqSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _
  private val finishLatch: CountDownLatch = new CountDownLatch(1)

  private val self = this
  private var forwardPushReqSO: StreamObserver[Proxy.Packet] = _
  private var failRequest: mutable.ParHashSet[Proxy.Metadata] = new mutable.ParHashSet[Proxy.Metadata]()

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    if (inited) return
    logTrace(s"ForwardPushReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    val dstPartyId = rsHeader.dstPartyId
    val endpoint = Router.query(dstPartyId).point
    var isSecure = Router.query(dstPartyId).isSecure
    val caCrt = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH.get()

    // use secure channel conditions:
    // 1 include crt file.
    // 2 packet have diff src and dst party.
    // 3 point is secure
    isSecure = if (!StringUtils.isBlank(caCrt)
      && firstRequest.getHeader.getDst.getPartyId != firstRequest.getHeader.getSrc.getPartyId) isSecure else false
    val channel = GrpcClientUtils.getChannel(endpoint, isSecure)
    val stub = DataTransferServiceGrpc.newStub(channel)
    forwardPushReqSO = stub.push(new ForwardPushRespSO(eggSiteServicerPushRespSO_forwardPushToPollingRespSO, finishLatch))

    inited = true
    logDebug(s"ForwardPushReqSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(request: Proxy.Packet): Unit = {
    try {
      logTrace(s"ForwardPushReqSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
      ensureInited(request)

      val nextReq = if (TransferExceptionUtils.checkPacketIsException(request)) {
        val packet = TransferExceptionUtils.genExceptionToNextSite(request)
        val errStack = request.getBody.getValue.toStringUtf8
        logError(s"ForwardPushReqSO.onNext with error received from prev. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}. ${errStack}")
        packet
      } else {
        request
      }

      forwardPushReqSO.onNext(nextReq)
      logTrace(s"ForwardPushReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    } catch {
      case t: Throwable =>
        // exception transfer of per packet only try once
        logError(s"ForwardPushReqSO.onNext with error occurred in processing. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)

        if (!failRequest.contains(request.getHeader)) {
          failRequest += request.getHeader

          val rsException = TransferExceptionUtils.throwableToException(t)
          logTrace(s"ForwardPushReqSO.onNext passing error to nextReqSO via onNext. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          forwardPushReqSO.onNext(TransferExceptionUtils.genExceptionToNextSite(request, rsException))
        }
        failRequest -= request.getHeader
        // notifying prev via normal onError
        onError(t)
    }
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"ForwardPushReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onError(wrapped)
    logError(s"ForwardPushReqSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPushReqSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    forwardPushReqSO.onCompleted()
    if (!finishLatch.await(RollSiteConfKeys.EGGROLL_ROLLSITE_ONCOMPLETED_WAIT_TIMEOUT.get().toLong, TimeUnit.SECONDS)) {
      onError(new TimeoutException(s"ForwardPushReqSO.onCompleted latch timeout. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}"))
    }
    logTrace(s"ForwardPushReqSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}


class ForwardPushRespSO(val eggSiteServicerPushRespSO_forwardPushToPollingRespSO: StreamObserver[Proxy.Metadata],
                        val finishLatch: CountDownLatch)
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _

  override def onNext(resp: Proxy.Metadata): Unit = {
    metadata = resp
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)
    logTrace(s"ForwardPushRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
      rsKey = rsHeader.getRsKey()
      eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onNext(resp)
      eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onCompleted()
    } catch {
      case t: Throwable =>
        onError(t)
    }

    logTrace(s"ForwardPushRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"ForwardPushRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    eggSiteServicerPushRespSO_forwardPushToPollingRespSO.onError(wrapped)
    finishLatch.countDown()
    logError(s"ForwardPushRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPushRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    finishLatch.countDown()
    logTrace(s"ForwardPushRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}
