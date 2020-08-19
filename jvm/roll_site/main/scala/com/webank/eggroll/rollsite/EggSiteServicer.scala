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

import java.util.concurrent.{Future, ThreadPoolExecutor, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.protobuf.ByteString
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant.{RollSiteConfKeys, SerdesTypes, SessionConfKeys, StringConstants}
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage
import com.webank.eggroll.core.meta.{ErJob, ErRollSiteHeader, ErTask}
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader
import com.webank.eggroll.core.transfer.{GrpcClientUtils, Transfer, TransferServiceGrpc}
import com.webank.eggroll.core.util._
import com.webank.eggroll.rollpair.{RollPair, RollPairContext}
import io.grpc.stub.StreamObserver

import scala.collection.parallel.mutable

class DataTransferServicer extends DataTransferServiceGrpc.DataTransferServiceImplBase with Logging {

  /**
   */
  override def push(responseObserver: StreamObserver[Proxy.Metadata]): StreamObserver[Proxy.Packet] = {
    logInfo("[PUSH][SERVER] request received")
    new DelegateDispatchSO(responseObserver)
  }

  /**
   */
  override def unaryCall(request: Proxy.Packet,
                         responseSO: StreamObserver[Proxy.Packet]): Unit = {
    /**
     * Check if dst is myself.
     *   - yes -> check command to see what the request wants.
     *   - no -> forwards it to the next hop synchronously.
     */

    val metadata = request.getHeader
    val oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)
    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    val rsKey = rollSiteHeader.getRsKey()

    try {
      val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
      val dstPartyId = metadata.getDst.getPartyId

      val logMsg = s"[UNARYCALL][SERVER] unaryCall request received. rsKey=${rsKey}, metadata=${oneLineStringMetadata}"

      val result = if (myPartyId.equals(dstPartyId)) {
        logDebug(s"${logMsg}, hop=SINK")
        request
        //processCommand(request)
      } else {
        logDebug(s"${logMsg}, hop=FORWARD")

        val endpoint = Router.query(myPartyId)
        val channel = GrpcClientUtils.getChannel(endpoint)
        val stub = DataTransferServiceGrpc.newBlockingStub(channel)

        stub.unaryCall(request)
      }

      responseSO.onNext(result)
      responseSO.onCompleted()
    } catch {
      case t: Throwable =>
        logError(s"[UNARYCALL][SERVER] onError. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
        val wrapped = ExceptionTransferHelp.throwableToException(t)
        responseSO.onError(wrapped)
    }
  }

  // processes commands e.g. getStatus, pushObj, pullObj etc.
  private def processCommand(request: Proxy.Packet): Proxy.Packet = {
    logInfo(s"packet to myself. response: ${ToStringUtils.toOneLineString(request)}")

    val header = request.getHeader
    val operator = header.getOperator


    if (operator.equals("init_job_session_pair")) doInitJobSessionPair(request)
    else throw new UnsupportedOperationException(s"operation ${operator} not supported")
  }

  private def doInitJobSessionPair(request: Proxy.Packet): Proxy.Packet = {
    val header = request.getHeader
    val pairInfo = header.getTask.getModel

    val jobId = pairInfo.getName
    if (!DataTransferServicer.jobIdToSession.asMap().containsKey(jobId)) {
      val erSessionId = pairInfo.getDataKey
      val erSession = new ErSession(sessionId = erSessionId, createIfNotExists = false)

      DataTransferServicer.jobIdToSession.put(jobId, erSession)
    }

    request.toBuilder
      .setHeader(header.toBuilder.setAck(header.getSeq))
      .build()
  }
}

object DataTransferServicer {
  val dataTransferServerExecutor: ThreadPoolExecutor =
    ThreadPoolUtils.newCachedThreadPool("data-transfer-server")
  val dataTransferClientExecutor: ThreadPoolExecutor =
    ThreadPoolUtils.newCachedThreadPool("data-transfer-client")

  val jobIdToSession: LoadingCache[String, ErSession] = CacheBuilder.newBuilder
    .maximumSize(100000)
    .expireAfterAccess(60, TimeUnit.HOURS)
    .concurrencyLevel(50)
    .recordStats
    .build(new CacheLoader[String, ErSession]() {
      override def load(key: String): ErSession = {
        throw new IllegalAccessException("this cache cannot be loaded")
      }
    })
}


/************ Observers ************/

class DelegateDispatchSO(prevRespSO: StreamObserver[Proxy.Metadata]) extends StreamObserver[Proxy.Packet] with Logging {
  private var delegateSO: StreamObserver[Proxy.Packet] = _
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var encodedRollSiteHeader: ByteString = _
  private var rollSiteHeader: ErRollSiteHeader = _
  private var rsKey: String = _

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    encodedRollSiteHeader = metadata.getExt
    rollSiteHeader = RollSiteHeader.parseFrom(encodedRollSiteHeader).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
    val dstPartyId = firstRequest.getHeader.getDst.getPartyId

    var logMsg = s"[DELEGATE][SERVER] onInit. rsKey=${rsKey}, metadata=${oneLineStringMetadata}"
    delegateSO = if (myPartyId.equals(dstPartyId)) {
      if (isLogTraceEnabled()) {
        logTrace(s"${logMsg}, hop=SINK")
      }
      new PutBatchSinkRequestStreamObserver(prevRespSO)
    } else {
      if (isLogTraceEnabled()) {
        logTrace(s"${logMsg}, hop=FORWARD")
      }
      new ForwardReqSO(prevRespSO)
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

class MockingSinkRequestStreamObserver(respSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var response: Proxy.Metadata = _
  private var rollSiteHeader: ErRollSiteHeader = _

  override def onNext(request: Proxy.Packet): Unit = {
    logInfo(s"sink on next: ${ToStringUtils.toOneLineString(request)}")
  }

  override def onError(throwable: Throwable): Unit = {
    // error logic here
    logError(throwable)
  }

  override def onCompleted(): Unit = {
    // complete logic here
    respSO.onNext(Proxy.Metadata.getDefaultInstance)
    respSO.onCompleted()

    logInfo("completed")
  }
}


class PutBatchSinkRequestStreamObserver(prevRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false
  //private var ctx: RollPairContext = _

  // TODO:1: abstract the following 5 variables with other SOs
  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var encodedRollSiteHeader: ByteString = _
  private var rollSiteHeader: ErRollSiteHeader = _
  private var rsKey: String = _

  private val self = this
  private var nextReqSO: StreamObserver[Transfer.TransferBatch] = _

  private var brokerTag: String = _

  private val transferBatchBuilder = Transfer.TransferBatch.newBuilder()
  private val transferHeaderBuilder = Transfer.TransferHeader.newBuilder()
    .setTotalSize(-1L)
    .setStatus("stream_partition")

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    encodedRollSiteHeader = metadata.getExt
    rollSiteHeader = RollSiteHeader.parseFrom(encodedRollSiteHeader).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    val sessionId = String.join("_", rollSiteHeader.rollSiteSessionId, rollSiteHeader.dstRole, rollSiteHeader.dstPartyId)
    val session = new ErSession(sessionId, createIfNotExists = false)

    val namespace = rollSiteHeader.rollSiteSessionId
    val name = rsKey
    val ctx = new RollPairContext(session)

    var rpOptions = rollSiteHeader.options ++ Map(StringConstants.TOTAL_PARTITIONS_SNAKECASE -> rollSiteHeader.totalPartitions.toString)
    if (rollSiteHeader.dataType.equals("object")) rpOptions ++= Map(StringConstants.SERDES -> SerdesTypes.EMPTY)

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

    nextReqSO = stub.send(new PutBatchSinkRespSO(metadata, commandFuture, prevRespSO, nextReqSO))

    logDebug(s"[SINK][SERVER] ensureInited. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    inited = true
  }


  override def onNext(request: Proxy.Packet): Unit = {
    ensureInited(request)
    if (isLogTraceEnabled()) {
      logTrace(s"[SINK][SERVER] onNext. rsKey=${rsKey}, metadata=${oneLineStringMetadata}, data_size=${request.getBody.getValue.size()}")
    }

    try {
      if (ExceptionTransferHelp.checkPacketIsException(request)) {
        val errStack = request.getBody.getValue.toStringUtf8
        logError(s"[SINK][SERVER] error received from prev. rsKey=${rsKey}, metadata=${oneLineStringMetadata}. ${errStack}")
        // todo:0: error handing
      } else {
        val tbHeader = transferHeaderBuilder.setId(metadata.getSeq.toInt)
          .setTag(brokerTag)
          .setExt(request.getHeader.getExt)
          .setTotalSize(rollSiteHeader.options.getOrElse("stream_batch_count", "-1").toLong)

        val tbBatch = transferBatchBuilder.setHeader(tbHeader)
          .setData(request.getBody.getValue)
          .build()

        nextReqSO.onNext(tbBatch)
      }
    } catch {
      case t: Throwable => {
        onError(t)
      }
    }
  }

  override def onError(t: Throwable): Unit = {
    val statusException = ExceptionTransferHelp.throwableToException(t)
    logError(s"[SINK][SERVER] onError. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", statusException)
    prevRespSO.onError(statusException)

    if (isLogTraceEnabled()) {
      logError(s"[SINK][SERVER] onError finished. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    }
  }

  override def onCompleted(): Unit = {
    if (isLogTraceEnabled()) {
      logTrace(s"[SINK][SERVER] onComplete received. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    }
    nextReqSO.onCompleted()
    logDebug(s"[SINK][SERVER] onComplete finished. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

class PutBatchSinkRespSO(val reqHeader: Proxy.Metadata,
                                         val commandFuture: Future[ErTask],
                                         val prevRespSO: StreamObserver[Proxy.Metadata],
                                         val nextReqSO: StreamObserver[Transfer.TransferBatch])
  extends StreamObserver[Transfer.TransferBatch] with Logging {

  private var transferHeader: Transfer.TransferHeader = _
  private var oneLineStringTransferHeader: String = _

  private var rsKey: String = _

  override def onNext(resp: Transfer.TransferBatch): Unit = {
    transferHeader = resp.getHeader
    oneLineStringTransferHeader = ToStringUtils.toOneLineString(transferHeader)

    val rollSiteHeader = RollSiteHeader.parseFrom(transferHeader.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    if (isLogTraceEnabled()) {
      logTrace(s"[SINK][CLIENT] onNext received. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
    }
    prevRespSO.onNext(reqHeader.toBuilder.setAck(resp.getHeader.getId).build())

    if (isLogTraceEnabled()) {
      logTrace(s"[SINK][CLIENT] onNext finished. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
    }
  }

  override def onError(t: Throwable): Unit = {
    val e = ExceptionTransferHelp.throwableToException(t)
    logError(s"[SINK][CLIENT] onError. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}", e)
    prevRespSO.onError(e)
    //nextReqSO.onError(e)
  }

  override def onCompleted(): Unit = {
    if (isLogTraceEnabled()) {
      logTrace(s"[SINK][CLIENT] onComplete received. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
    }
    commandFuture.get()
    prevRespSO.onCompleted()

    if (isLogTraceEnabled()) {
      logTrace(s"[SINK][CLIENT] onComplete finished. rsKey=${rsKey}, transferHeader=${oneLineStringTransferHeader}")
    }
  }
}


class ForwardReqSO(prevRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _

  private val self = this
  private var nextReqSO: StreamObserver[Proxy.Packet] = _
  private var failRequest: mutable.ParHashSet[Proxy.Metadata] = new mutable.ParHashSet[Proxy.Metadata]()

  private def ensureInited(firstRequest: Proxy.Packet): Unit = {
    if (inited) return

    metadata = firstRequest.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()

    logDebug(s"[FORWARD][SERVER] onInit. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    val dstPartyId = rollSiteHeader.dstPartyId
    val endpoint = Router.query(dstPartyId)
    val channel = GrpcClientUtils.getChannel(endpoint)
    val stub = DataTransferServiceGrpc.newStub(channel)
    nextReqSO = stub.push(new ForwardRespSO(prevRespSO))

    inited = true
  }

  override def onNext(request: Proxy.Packet): Unit = {
    try {
      ensureInited(request)
      if (isLogTraceEnabled()) {
        logTrace(s"[FORWARD][SERVER] onNext. rsKey=${rsKey}, metadata=${metadata}")
      }

      val nextReq = if (ExceptionTransferHelp.checkPacketIsException(request)) {
        val packet = ExceptionTransferHelp.genExceptionToNextSite(request)
        val errStack = request.getBody.getValue.toStringUtf8
        logError(s"[FORWARD][SERVER] error received from prev. rsKey=${rsKey}, metadata=${oneLineStringMetadata}. ${errStack}")
        packet
      } else {
        request
      }

      /*
      // exception transfer test
      if (System.currentTimeMillis() % 4 == 0) {
        val test: Array[Int] = Array()
        println(test(20))
      }
      */

      nextReqSO.onNext(nextReq)
    } catch {
      case t: Throwable =>
        // exception transfer of per packet only try once
        logError(s"[FORWARD][SERVER] error occurred in onNext. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)

        if (!failRequest.contains(request.getHeader)) {
          failRequest += request.getHeader

          val rsException = ExceptionTransferHelp.throwableToException(t)
          logTrace(s"[FORWARD][SERVER] passing error to nextReqSO via onNext. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
          nextReqSO.onNext(ExceptionTransferHelp.genExceptionToNextSite(request, t))


        }
        failRequest -= request.getHeader
        // notifying prev via normal onError
        onError(t)
    }
  }

  override def onError(t: Throwable): Unit = {
    logError(s"[FORWARD][SERVER] onError. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    val e = ExceptionTransferHelp.throwableToException(t)
    prevRespSO.onError(e)
  }

  override def onCompleted(): Unit = {
    nextReqSO.onCompleted()
    logDebug(s"[FORWARD][SERVER] onCompleted. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
  }
}

class ForwardRespSO(val prevRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _

  private var rsKey: String = _

  override def onNext(resp: Proxy.Metadata): Unit = {
    metadata = resp
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    val rollSiteHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rollSiteHeader.getRsKey()
    if (isLogTraceEnabled()) {
      logTrace(s"[FORWARD][CLIENT] onNext. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    }
    prevRespSO.onNext(resp)
  }

  override def onError(t: Throwable): Unit = {
    logError(s"[FORWARD][CLIENT] onError received. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
    val e = ExceptionTransferHelp.throwableToException(t)
    prevRespSO.onError(e)
    logError(s"[FORWARD][CLIENT] onError finished. rsKey=${rsKey}, metadata=${oneLineStringMetadata}", t)
  }

  override def onCompleted(): Unit = {
    if (isLogTraceEnabled()) {
      logTrace(s"[FORWARD][CLIENT] onComplete received. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    }
    prevRespSO.onCompleted()

    if (isLogTraceEnabled()) {
      logTrace(s"[FORWARD][CLIENT] onComplete finished. rsKey=${rsKey}, metadata=${oneLineStringMetadata}")
    }
  }
}