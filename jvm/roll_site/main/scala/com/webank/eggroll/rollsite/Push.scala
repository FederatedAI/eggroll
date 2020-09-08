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

import java.util.concurrent.Future

import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.meta.{ErJob, ErTask}
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.transfer.{GrpcClientUtils, Transfer, TransferServiceGrpc}
import com.webank.eggroll.core.util.{IdUtils, Logging, ToStringUtils}
import com.webank.eggroll.rollpair.{RollPair, RollPairContext}
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils

import scala.collection.parallel.mutable


class DispatchPushReqSO(eggSiteServicerPushRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var delegateSO: StreamObserver[Proxy.Packet] = _
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _

  private def ensureInited(req: Proxy.Packet): Unit = {
    if (inited) return

    metadata = req.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val encodedRollSiteHeader = metadata.getExt
    val rollSiteHeader = RollSiteHeader.parseFrom(encodedRollSiteHeader).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
    val dstPartyId = req.getHeader.getDst.getPartyId

    val logMsg = s"DispatchPushReqSO.ensureInited. rsKey=${rsKey}, metadata=${oneLineStringMetadata}"
    delegateSO =  if (myPartyId.equals(dstPartyId)) {
      if (isLogTraceEnabled()) {
        logTrace(s"${logMsg}, hop=SINK")
      }
      new PutBatchSinkPushReqSO(eggSiteServicerPushRespSO)
    } else {
      if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_SERVER_ENABLED.get().toBoolean) {
      // polling mode
        logTrace(s"${logMsg}, hop=FORWARD, type=PUSH2POLLING")
        new ForwardPushToPollingReqSO(eggSiteServicerPushRespSO)
      } else {
        logTrace(s"${logMsg}, hop=FORWARD, type=PUSH2PUSH")
        new ForwardPushReqSO(eggSiteServicerPushRespSO)
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


class PutBatchSinkPushReqSO(eggSiteServicerPushRespSO_putBatchPollingPushRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false
  //private var ctx: RollPairContext = _

  // TODO:1: abstract the following 5 variables with other SOs
  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _

  private val self = this
  private var putBatchSinkPushReqSO: StreamObserver[Transfer.TransferBatch] = _

  private var brokerTag: String = _

  private val transferBatchBuilder = Transfer.TransferBatch.newBuilder()
  private val transferHeaderBuilder = Transfer.TransferHeader.newBuilder()
    .setTotalSize(-1L)
    .setStatus("stream_partition")

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    logTrace(s"PutBatchSinkPushReqSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val encodedRollSiteHeader = metadata.getExt
    val rollSiteHeader = RollSiteHeader.parseFrom(encodedRollSiteHeader).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    val sessionId = String.join("_", rollSiteHeader.rollSiteSessionId, rollSiteHeader.dstRole, rollSiteHeader.dstPartyId)
    val session = new ErSession(sessionId, createIfNotExists = false)

    val namespace = rollSiteHeader.rollSiteSessionId
    val name = rsKey
    val ctx = new RollPairContext(session)

    var rpOptions = rollSiteHeader.options ++ Map(StringConstants.TOTAL_PARTITIONS_SNAKECASE -> rollSiteHeader.totalPartitions.toString)
    if (rollSiteHeader.dataType.equals("object")) rpOptions ++= Map(StringConstants.SERDES -> SerdesTypes.EMPTY)

    // table creates here
    val rp = ctx.load(namespace, name, options = rpOptions)

    val partitionId = rollSiteHeader.partitionId
    val partition = rp.store.partitions(partitionId)
    val egg = ctx.session.routeToEgg(partition)

    brokerTag = s"${RollPair.PUT_BATCH}-${rollSiteHeader.getRsKey()}-${partitionId}"

    val jobId = IdUtils.generateJobId(sessionId = ctx.session.sessionId, tag = brokerTag)
    val job = ErJob(
      id = jobId,
      name = RollPair.PUT_BATCH,
      inputs = Array(rp.store),
      outputs = Array(rp.store),
      functors = Array.empty,
      options = rollSiteHeader.options ++ Map(SessionConfKeys.CONFKEY_SESSION_ID -> ctx.session.sessionId))

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

    putBatchSinkPushReqSO = stub.send(new PutBatchSinkPushRespSO(metadata, commandFuture, eggSiteServicerPushRespSO_putBatchPollingPushRespSO))

    inited = true
    logDebug(s"PutBatchSinkPushReqSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(request: Proxy.Packet): Unit = {
    logTrace(s"PutBatchSinkPushReqSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}, data_size=${request.getBody.getValue.size()}")
    ensureInited(request)

    try {
      if (TransferExceptionUtils.checkPacketIsException(request)) {
        val errStack = request.getBody.getValue.toStringUtf8
        logError(s"PutBatchSinkPushReqSO.onNext error received from prev. rsKey=${rsKey}, metadata=${oneLineStringMetadata}, stack=${errStack}")
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
    logTrace(s"PutBatchSinkPushReqSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PutBatchSinkPushReqSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    val statusException = TransferExceptionUtils.throwableToException(t)
    eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onError(statusException)
    logError(s"PutBatchSinkPushReqSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PutBatchSinkPushReqSO.onComplete calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    putBatchSinkPushReqSO.onCompleted()
    logTrace(s"PutBatchSinkPushReqSO.onComplete called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

class PutBatchSinkPushRespSO(val reqHeader: Proxy.Metadata,
                             val commandFuture: Future[ErTask],
                             val eggSiteServicerPushRespSO_putBatchPollingPushRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Transfer.TransferBatch] with Logging {

  private var transferHeader: Transfer.TransferHeader = _
  private var oneLineStringTransferHeader: String = _

  private var rsKey: String = _

  override def onNext(resp: Transfer.TransferBatch): Unit = {
    logTrace(s"PutBatchSinkPushRespSO.onNext calling. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
    transferHeader = resp.getHeader
    oneLineStringTransferHeader = ToStringUtils.toOneLineString(transferHeader)

    val rollSiteHeader = RollSiteHeader.parseFrom(transferHeader.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onNext(reqHeader.toBuilder.setAck(resp.getHeader.getId).build())
    logDebug(s"PutBatchSinkPushRespSO.onNext called. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"PutBatchSinkPushRespSO.onError calling. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}", t)
    val e = TransferExceptionUtils.throwableToException(t)
    eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onError(e)
    logError(s"PutBatchSinkPushRespSO.onError called. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}", t)
  }

  override def onCompleted(): Unit = {
    logTrace(s"PutBatchSinkPushRespSO.onComplete calling. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
    commandFuture.get()
    eggSiteServicerPushRespSO_putBatchPollingPushRespSO.onCompleted()
    logTrace(s"PutBatchSinkPushRespSO.onComplete called. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
  }
}


class ForwardPushToPollingReqSO(eggSiteServicerPushRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _

  private val self = this
  private var failRequest: mutable.ParHashSet[Proxy.Metadata] = new mutable.ParHashSet[Proxy.Metadata]()
  private val pollingFrameBuilder = Proxy.PollingFrame.newBuilder().setMethod("push")
  private var pollingFrameSeq = 0
  private val pollingExchanger = PollingHelper.pollingExchangerQueue.take()

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    logTrace(s"ForwardPushToPollingReqSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    pollingExchanger.setMethod(PollingMethods.PUSH)
    inited = true
    logDebug(s"ForwardPushToPollingReqSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.Packet): Unit = {
    logTrace(s"ForwardPushToPollingReqSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    ensureInited(req)

    pollingFrameSeq += 1
    pollingFrameBuilder.setSeq(pollingFrameSeq).setPacket(req)
    val nextFrame = pollingFrameBuilder.build()

    pollingExchanger.respQ.put(nextFrame)
    logTrace(s"ForwardPushToPollingReqSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"ForwardPushToPollingReqSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    eggSiteServicerPushRespSO.onError(TransferExceptionUtils.throwableToException(t))
    logError(s"ForwardPushToPollingReqSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPushToPollingReqSO.onCompleted calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    pollingFrameSeq += 1
    pollingFrameBuilder.setSeq(pollingFrameSeq).setMethod("finish_push")
    pollingExchanger.respQ.put(pollingFrameBuilder.build())

    val pollingReq = pollingExchanger.reqQ.take()

    eggSiteServicerPushRespSO.onNext(pollingReq.getMetadata)
    eggSiteServicerPushRespSO.onCompleted()

    logTrace(s"ForwardPushToPollingReqSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}


class ForwardPushReqSO(eggSiteServicerPushRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _

  private val self = this
  private var forwardPushReqSO: StreamObserver[Proxy.Packet] = _
  private var failRequest: mutable.ParHashSet[Proxy.Metadata] = new mutable.ParHashSet[Proxy.Metadata]()

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    logTrace(s"ForwardPushReqSO.ensureInited calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    val dstPartyId = rollSiteHeader.dstPartyId
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
    forwardPushReqSO = stub.push(new ForwardPushRespSO(eggSiteServicerPushRespSO))

    inited = true
    logDebug(s"ForwardPushReqSO.ensureInited called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(request: Proxy.Packet): Unit = {
    try {
      logTrace(s"ForwardPushReqSO.onNext calling. rsKey=${rsKey}, metadata=${metadata}")
      ensureInited(request)

      val nextReq = if (TransferExceptionUtils.checkPacketIsException(request)) {
        val packet = TransferExceptionUtils.genExceptionToNextSite(request)
        val errStack = request.getBody.getValue.toStringUtf8
        logError(s"ForwardPushReqSO.onNext with error received from prev. rsKey=${rsKey}, metadata=${oneLineStringMetadata}. ${errStack}")
        packet
      } else {
        request
      }

      forwardPushReqSO.onNext(nextReq)
      logTrace(s"ForwardPushReqSO.onNext called. rsKey=${rsKey}, metadata=${metadata}")
    } catch {
      case t: Throwable =>
        // exception transfer of per packet only try once
        logError(s"ForwardPushReqSO.onNext with error occurred in processing. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)

        if (!failRequest.contains(request.getHeader)) {
          failRequest += request.getHeader

          val rsException = TransferExceptionUtils.throwableToException(t)
          logTrace(s"ForwardPushReqSO.onNext passing error to nextReqSO via onNext. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
          forwardPushReqSO.onNext(TransferExceptionUtils.genExceptionToNextSite(request, t))
        }
        failRequest -= request.getHeader
        // notifying prev via normal onError
        onError(t)
    }
  }

  override def onError(t: Throwable): Unit = {
    logError(s"ForwardPushReqSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    val e = TransferExceptionUtils.throwableToException(t)
    eggSiteServicerPushRespSO.onError(e)
    logError(s"ForwardPushReqSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPushReqSO.onCompleted calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    forwardPushReqSO.onCompleted()
    logTrace(s"ForwardPushReqSO.onCompleted called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}


class ForwardPushRespSO(val eggSiteServicerPushRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _

  override def onNext(resp: Proxy.Metadata): Unit = {
    metadata = resp
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)
    logTrace(s"ForwardPushRespSO.onNext calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()
    eggSiteServicerPushRespSO.onNext(resp)
    logTrace(s"ForwardPushRespSO.onNext called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    logError(s"ForwardPushRespSO.onError calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    val e = TransferExceptionUtils.throwableToException(t)
    eggSiteServicerPushRespSO.onError(e)
    logError(s"ForwardPushRespSO.onError called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPushRespSO.onComplete calling. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    eggSiteServicerPushRespSO.onCompleted()
    logTrace(s"ForwardPushRespSO.onComplete called. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}
