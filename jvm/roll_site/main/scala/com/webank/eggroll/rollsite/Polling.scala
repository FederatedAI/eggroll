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
import com.webank.eggroll.core.util.{ErrorUtils, Logging, ToStringUtils}
import com.webank.eggroll.rollsite.PollingResults.errorPoison
import io.grpc.ConnectivityState
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import javax.security.sasl.AuthenticationException
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.concurrent.TimeoutException


object PollingMethods {
  val PUSH = "push"
  val SUB_STREAM_FINISH = "sub_stream_finish"
  val UNARY_CALL = "unary_call"
  val FINISH_UNARY_CALL = "finish_unary_call"
  val COMPLETED_POISON = "completed_poison"
  val NO_DATA_POISON = "no_data_poison"
  val ERROR_POISON = "error_poison"
  val MOCK = "mock"
}

object LongPollingClient extends Logging {
  private var defaultPollingReqMetadata: Proxy.Metadata = Proxy.Metadata.newBuilder()
    .setDst(
      Proxy.Topic.newBuilder()
        .setPartyId(RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()))
    .build()

  var initPollingFrameBuilder: Proxy.PollingFrame.Builder = Proxy.PollingFrame.newBuilder().setMetadata(defaultPollingReqMetadata)

  val pollingAuthenticationEnabled = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AHTHENTICATION_ENABLED.get().toBoolean

  private val pollingSemaphore = new Semaphore(RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_CONCURRENCY.get().toInt)

  private var shouldPollingContinue: Boolean = true

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

      if (LongPollingClient.pollingAuthenticationEnabled) {
        val pollingAuthenticator = Class.forName("com.webank.eggroll.rollsite.FatePollingAuthenticator")
          .newInstance().asInstanceOf[PollingAuthenticator]

        LongPollingClient.defaultPollingReqMetadata = Proxy.Metadata.newBuilder()
          .setDst(
            Proxy.Topic.newBuilder()
              .setPartyId(LongPollingClient.defaultPollingReqMetadata.getDst.getPartyId))
          .setTask(Proxy.Task.newBuilder()
            .setModel(Proxy.Model.newBuilder()
              .setDataKey(pollingAuthenticator.sign())))
          .build()

        LongPollingClient.initPollingFrameBuilder = Proxy.PollingFrame.newBuilder().setMetadata(LongPollingClient.defaultPollingReqMetadata)
        logTrace(s"authInfo to be sent=${LongPollingClient.defaultPollingReqMetadata.getTask.getModel.getDataKey}, " +
          s"partyID=${LongPollingClient.initPollingFrameBuilder.getMetadata.getDst.getPartyId}")
      }

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
        //throw new RuntimeException("mocking error")
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
              finished = true
            case PollingMethods.NO_DATA_POISON =>
              throw new CancellationException("polling timeout with no data")
            case PollingMethods.ERROR_POISON =>
              throw pollingResults.getError()
            case PollingMethods.MOCK =>
              logDebug(s"mocking response: ${ToStringUtils.toOneLineString(req)}")
            case _ =>
              throw new NotImplementedError(s"received unknown method: ${req.getMethod}")
          }
        }
      } catch {
        case t: CancellationException =>
          logDebug(t.getMessage)
          //pollingReqSO.onNext(TransferExceptionUtils.genExceptionPollingFrame(t))
          pollingReqSO.onCompleted()
        case t: Throwable =>
          logError("polling with error", t)

          pollingReqSO.onNext(TransferExceptionUtils.genExceptionPollingFrame(t))
          pollingReqSO.onCompleted()

          // if exception contains AuthenticationException, it means anthInfo client sent did not pass server authentication
          if (ExceptionUtils.getStackTrace(t).contains(classOf[AuthenticationException].getSimpleName)) {
            logError(s"fail to authenticate authInfo=${LongPollingClient.initPollingFrameBuilder.getMetadata.getTask.getModel.getDataKey} " +
              s", please recheck your authInfo and restart rollsite, start to terminate polling")
            LongPollingClient.shouldPollingContinue = false
          }
          //pollingReqSO.onError(TransferExceptionUtils.throwableToException(t))
      } finally {
        if (!finishLatch.await(RollSiteConfKeys.EGGROLL_ROLLSITE_ONCOMPLETED_WAIT_TIMEOUT.get().toLong, TimeUnit.SECONDS)) {
          throw new TimeoutException(s"longPollingClient.onCompleted latch timeout")
        }
        logTrace("polling finally run completed")
      }

      // TODO:0: configurable
    } catch {
      case t: Throwable =>
        logError("polling failed", t)
    }
  }

  def pollingForever(): Unit = {
    while (LongPollingClient.shouldPollingContinue) {
      try {
        polling()
      } catch {
        case e: Throwable =>
          logError("polling failed", e)
          Thread.sleep(1211)
      }
    }
    logInfo("polling exit")
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

  def waitMethod(timeout: Long = -1L, unit: TimeUnit = TimeUnit.SECONDS): String = {
    if (timeout == -1L) methodLatch.await()
    else methodLatch.await(timeout, unit)
    method
  }
}

object PollingExchanger extends Logging {
  val pollingExchangerQueueMap = new java.util.concurrent.ConcurrentHashMap[String, LinkedBlockingQueue[PollingExchanger]]()

  def getPollingExchangerQueue(partyId: String): LinkedBlockingQueue[PollingExchanger] = {
    if (!pollingExchangerQueueMap.containsKey(partyId)) {
      this.synchronized{
        if (!pollingExchangerQueueMap.containsKey(partyId)) {
          val pollingExchangerQueue = new LinkedBlockingQueue[PollingExchanger]()
          pollingExchangerQueueMap.put(partyId, pollingExchangerQueue)
          logDebug(s"created pollingExchangerQueue for party=${partyId}")
        }
      }
    }
    pollingExchangerQueueMap.get(partyId)
  }

  def offer(data: Proxy.PollingFrame, q: SynchronousQueue[Proxy.PollingFrame], logPrefix: String, rsHeader: ErRollSiteHeader = null, metadataString: String = null): Boolean = {
    var done = false
    var curRetry = 0
    val interval = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_Q_OFFER_INTERVAL_SEC.get().toLong
    val timeout = System.currentTimeMillis() + RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC.get().toLong * 1000

    while (!done && System.currentTimeMillis() <= timeout) {
      done = q.offer(data, interval, TimeUnit.SECONDS)
      logTrace(s"${logPrefix} offering data, done=${done}, curRetry=${curRetry}, rsKey=${if (rsHeader != null) rsHeader.getRsKey() else "null"}, rsHeader=${rsHeader}, metadata=${metadataString}")
      curRetry += 1
    }

    if (!done) throw new TimeoutException(s"${logPrefix} failed: timeout. current EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC=${RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC.get()}")
    else done
  }

  def poll(q: SynchronousQueue[Proxy.PollingFrame], logPrefix: String, rsHeader: ErRollSiteHeader = null, metadataString: String = null): Proxy.PollingFrame = {
    var result: Proxy.PollingFrame = null
    var curRetry = 0
    val interval = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_Q_OFFER_INTERVAL_SEC.get().toLong
    val timeout = System.currentTimeMillis() + RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC.get().toLong * 1000

    while (result == null && System.currentTimeMillis() <= timeout) {
      result = q.poll(interval, TimeUnit.SECONDS)
      logTrace(s"${logPrefix} polling data, isNull=${result == null}, curRetry=${curRetry}, rsKey=${if (rsHeader != null) rsHeader.getRsKey() else "null"}, rsHeader=${rsHeader}, metadata=${metadataString}")
      curRetry += 1
    }

    if (result == null) throw new TimeoutException(s"${logPrefix} failed: timeout. current EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC=${RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC.get()}")
    else result
  }
}

class PollingResults() extends Iterator[Proxy.PollingFrame] with Logging {
  private val q = new LinkedBlockingQueue[Proxy.PollingFrame]()
  private val error: AtomicReference[Throwable] = new AtomicReference[Throwable](null)

  def put(f: Proxy.PollingFrame): Unit = {
    q.put(f)
  }

  def setFinish(): Unit = {
    q.put(PollingResults.completedPoison)
  }

  def setError(t: Throwable): Unit = {
    this.error.compareAndSet(null, t)
    q.put(errorPoison)
  }

  def getError(): Throwable = this.error.get()

  override def hasNext: Boolean = true

  override def next(): Proxy.PollingFrame = {
    // TODO:0: Configurable
    var result: Proxy.PollingFrame = q.poll(
      RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_NO_DATA_TIMEOUT_SEC.get().toLong, TimeUnit.SECONDS)

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
    if (inited) return
    logTrace(s"DispatchPollingReqSO.ensureInited calling.")

    val pollingAuthenticationEnabled = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AHTHENTICATION_ENABLED.get().toBoolean
    if (pollingAuthenticationEnabled) {
        val pollingAuthenticator = Class.forName(RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATOR_CLASS.get())
          .newInstance().asInstanceOf[PollingAuthenticator]

      val authResult = pollingAuthenticator.authenticate(req)
      if (authResult) {
        logTrace(s"polling authentication of party=${req.getMetadata.getDst.getPartyId} successful")
      } else {
        val errorInfo = new AuthenticationException(s"polling authentication of party=${req.getMetadata.getDst.getPartyId} failed, " +
          s"please check polling client authentication info=${req.getMetadata.getTask.getModel.getDataKey}")
        logError(s"polling authentication of party=${req.getMetadata.getDst.getPartyId} failed, please check polling client authentication info")
        throw new AuthenticationException(s"polling authentication of party=${req.getMetadata.getDst.getPartyId} failed, please check polling client authentication info")
      }
    } else {
      logDebug("polling authentication disabled")
    }

    pollingExchanger = new PollingExchanger()
    var done = false
    var i = 0
    val partyId = req.getMetadata.getDst.getPartyId
    val exchangerDataOpTimeout = System.currentTimeMillis() + RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_EXCHANGER_DATA_OP_TIMEOUT_SEC.get().toLong * 1000
    while (!done && System.currentTimeMillis() < exchangerDataOpTimeout) {
      done = PollingExchanger.getPollingExchangerQueue(partyId).offer(pollingExchanger,
        RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_Q_OFFER_INTERVAL_SEC.get().toLong, TimeUnit.SECONDS)
      logTrace(s"DispatchPollingReqSO.ensureInited calling, getting from pollingExchangerQueue. partyId=${partyId}, i=${i}")
      i += 1
    }

    if (!done) {
      onError(new TimeoutException(s"timeout when offering pollingExchanger to queue, partyId=${partyId}"))
      PollingExchanger.getPollingExchangerQueue(partyId).remove(pollingExchanger)
      return
    }
    // synchronise point for incoming push / unary_call request
    val timeout = System.currentTimeMillis() + RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_NO_DATA_TIMEOUT_SEC.get().toLong * 1000

    var method: String = null
    i = 0
    while (System.currentTimeMillis() < timeout && method == null) {
      method = pollingExchanger.waitMethod(60, TimeUnit.SECONDS)
      logTrace(s"DispatchPollingReqSO.ensureInited waiting method. i=${i}")
      i += 1
    }
    //val method = "mock"

    method match {
      case PollingMethods.PUSH =>
        delegateSO = new PushPollingReqSO(eggSiteServicerPollingRespSO, pollingExchanger)
      case PollingMethods.UNARY_CALL =>
        delegateSO = new UnaryCallPollingReqSO(eggSiteServicerPollingRespSO, pollingExchanger)
      case PollingMethods.MOCK =>
        delegateSO = new MockPollingReqSO(eggSiteServicerPollingRespSO)
      case null =>
        PollingExchanger.getPollingExchangerQueue(partyId).remove(pollingExchanger)
        throw new CancellationException(s"timeout in waiting polling method, partyId=${partyId}")
      case _ =>
        val e = new NotImplementedError(s"method ${method} not supported")
        logError(e)
        onError(e)
/*        eggSiteServicerPollingRespSO.onNext(TransferExceptionUtils.genExceptionPollingFrame(e))
        eggSiteServicerPollingRespSO.onCompleted()*/
    }

    inited = true
    logTrace(s"DispatchPollingReqSO.ensureInited called.")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    try {
      ensureInited(req)
      delegateSO.onNext(req)
    } catch {
      case t: Throwable =>
        onError(t)
    }
  }

  override def onError(t: Throwable): Unit = {
    if (t.isInstanceOf[CancellationException]) {
      logInfo(t.getMessage)
      eggSiteServicerPollingRespSO.onCompleted()
    } else if (delegateSO != null) {
      delegateSO.onError(t)
    } else {
      val wrapped = TransferExceptionUtils.throwableToException(t)
      logError("DispatchPollingReqSO.onError before init", wrapped)
      eggSiteServicerPollingRespSO.onError(wrapped)
    }
  }

  override def onCompleted(): Unit = {
    if (delegateSO != null) {
      delegateSO.onCompleted()
    } else {
      logWarning("DispatchPollingReqSO.onCompleted before init")
    }
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
    if (inited) return
    logTrace(s"UnaryCallPollingReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

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
      if (PollingMethods.ERROR_POISON.equals(req.getMethod))  {
        logError(s"UnaryCallPollingReqSO.onNext receives an error from pollingClient: ${req.getDesc}, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
        pollingExchanger.reqQ.clear()
        PollingExchanger.offer(req, pollingExchanger.reqQ, "UnaryCallPollingReqSO.onNext offering error to reqQ, ", rsHeader, oneLineStringMetadata)
        return
      }

      var batch: Proxy.PollingFrame = null
      req.getSeq match {
        case 0L =>
          logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=0L starting, " +
            s"rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

          batch = PollingExchanger.poll(pollingExchanger.respQ,
            "UnaryCallPollingReqSO.onNext req.getSeq=0L, ", rsHeader, oneLineStringMetadata)
          ensureInited(batch)

          eggSiteServicerPollingRespSO.onNext(batch)
          logTrace(s"UnaryCallPollingReqSO.onNext.req.getSeq=0L finished, " +
            s"rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
        case 1L =>
          logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=1L starting, " +
            s"rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          val pollingFrame = Proxy.PollingFrame.newBuilder().setPacket(req.getPacket).build()

          PollingExchanger.offer(pollingFrame, pollingExchanger.reqQ,
            "UnaryCallPollingReqSO.onNext req.getSeq=1L, ", rsHeader, oneLineStringMetadata)

          logTrace(s"UnaryCallPollingReqSO.onNext req.getSeq=1L finished, " +
            s"rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
        case _ =>
          val t: Throwable = new IllegalStateException(s"invalid seq=${req.getSeq} for rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          throw t
      }
    } catch {
      case t:Throwable =>
        onError(t)
    }

    logTrace(s"UnaryCallPollingReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"UnaryCallPollingReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    eggSiteServicerPollingRespSO.onError(wrapped)
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
    if (inited) return
    logTrace(s"PushPollingReqSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

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
      if (PollingMethods.ERROR_POISON.equals(req.getMethod))  {
        logError(s"PushPollingReqSO.onNext receives an error from pollingClient: ${req.getDesc}, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

        PollingExchanger.offer(req, pollingExchanger.reqQ,
          "PushPollingReqSO.onNext offering error to reqQ, ", rsHeader, oneLineStringMetadata)

        return
      }

      req.getSeq match {
        case 0L =>
          logTrace(s"PushPollingReqSO.onNext req.getSeq=0L starting, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          var isSubStreamFinished = false
          var batch: Proxy.PollingFrame = null

          while (!isSubStreamFinished) {
            batch = PollingExchanger.poll(pollingExchanger.respQ,
              "PushPollingReqSO.onNext req.getSeq=0L, polling from pollingExchanger.respQ", rsHeader, oneLineStringMetadata)

            ensureInited(batch)

            if (batch.getMethod.equals(PollingMethods.SUB_STREAM_FINISH)) {
              isSubStreamFinished = true
            }
            eggSiteServicerPollingRespSO.onNext(batch)
          }
          logTrace(s"PushPollingReqSO.onNext req.getSeq=0L finished, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
        case 1L =>
          logTrace(s"PushPollingReqSO.onNext req.getSeq=1L starting, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

          val metadataFrame = Proxy.PollingFrame.newBuilder().setMetadata(req.getMetadata).build()

          PollingExchanger.offer(metadataFrame, pollingExchanger.reqQ,
            "PushPollingReqSO.onNext req.getSeq=1L, ", rsHeader, oneLineStringMetadata)

          logTrace(s"PushPollingReqSO.onNext req.getSeq=1L finished, rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
        case _ =>
          val t: Throwable = new IllegalStateException(s"PushPollingReqSO.error: invalid seq=${req.getSeq} for rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
          onError(t)
      }
    } catch {
      case t: Throwable =>
        onError(t)
    }
    logTrace(s"PushPollingReqSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"PushPollingReqSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    eggSiteServicerPollingRespSO.onError(wrapped)
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
    if (inited) return
    logTrace(s"DispatchPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    method = req.getMethod

    method match {
      case PollingMethods.PUSH =>
        metadata = req.getPacket.getHeader
        delegateSO = new PushPollingRespSO(pollingResults)
      case PollingMethods.UNARY_CALL =>
        metadata = req.getPacket.getHeader
        delegateSO = new UnaryCallPollingRespSO(pollingResults)
      case PollingMethods.MOCK =>
        metadata = req.getPacket.getHeader
        delegateSO = new MockPollingRespSO(pollingResults)
      case _ =>
        val wrapped = TransferExceptionUtils.throwableToException(new NotImplementedError(s"operation ${method} not supported"))
        logError("fail to dispatch response", wrapped)
        onError(wrapped)
        throw wrapped
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
        val errMsg = s"DispatchPollingRespSO get error from push or unarycall. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}, stack:\n ${errStack}"
        logError(errMsg)
        throw new RuntimeException(errMsg)
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
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logTrace(s"DispatchPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    val calledMsg = s"DispatchPollingRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}"
    if (delegateSO != null) {
      delegateSO.onError(wrapped)
      finishLatch.countDown()
      LongPollingClient.releaseSemaphore()
      logTrace(calledMsg)
    } else {
      pollingResults.setError(t)
      finishLatch.countDown()
      LongPollingClient.releaseSemaphore()
      logError(s"${calledMsg}, delegateSO=null", t)
    }
  }

  override def onCompleted(): Unit = {
    logTrace(s"DispatchPollingRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (delegateSO != null) {
      delegateSO.onCompleted()
    }
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
  private val finishLatch = new CountDownLatch(1)

  private var pushReqSO: StreamObserver[Proxy.Packet] = _
  private var forwardPollingToPushRespSO: StreamObserver[Proxy.Metadata] = _

  private val self = this

  private def ensureInit(req: Proxy.PollingFrame): Unit = {
    if (inited) return
    logTrace(s"PushPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    method = req.getMethod
    metadata = req.getPacket.getHeader
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    forwardPollingToPushRespSO = new ForwardPushToPollingRespSO(pollingResults, finishLatch)
      // new PutBatchPollingPushRespSO(pollingResults)
    pushReqSO = new DispatchPushReqSO(forwardPollingToPushRespSO)
      // new PutBatchSinkPushReqSO(putBatchPollingPushRespSO)

    inited = true
    logDebug(s"PushPollingRespSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(req: Proxy.PollingFrame): Unit = {
    logTrace(s"PushPollingRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    try {
      ensureInit(req)

      val t = pollingResults.getError()
      if (t != null) {
        throw t
      } else {
        if (req.getMethod != PollingMethods.SUB_STREAM_FINISH) {
          pushReqSO.onNext(req.getPacket)
        } else {
          pushReqSO.onCompleted()
        }
      }
    } catch {
      case t: Throwable =>
        onError(t)
    }

    logTrace(s"PushPollingRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"PushPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    pollingResults.setError(wrapped)
    pushReqSO.onError(wrapped)
    logError(s"PushPollingRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"PushPollingRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    if (!finishLatch.await(RollSiteConfKeys.EGGROLL_ROLLSITE_ONCOMPLETED_WAIT_TIMEOUT.get().toLong, TimeUnit.SECONDS)) {
      onError(new TimeoutException(s"PushPollingRespSO.onCompleted latch timeout"))
    }
    logTrace(s"PushPollingRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }
}

/**
 * Polling rs gets push resp and pass
 */
class ForwardPushToPollingRespSO(pollingResults: PollingResults,
                                 finishLatch: CountDownLatch)
  extends StreamObserver[Proxy.Metadata] with Logging {

  private var inited = false

  private var metadata: Proxy.Metadata = _
  private var oneLineStringMetadata: String = _
  private var rsKey: String = _
  private var rsHeader: ErRollSiteHeader = _
  private var method: String = _

  private var pollingFrameSeq = 0

  private def ensureInited(req: Proxy.Metadata): Unit = {
    if (inited) return
    logTrace(s"ForwardPushToPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

    metadata = req
    oneLineStringMetadata = ToStringUtils.toOneLineString(metadata)

    rsHeader = RollSiteHeader.parseFrom(metadata.getExt).fromProto()
    rsKey = rsHeader.getRsKey()

    inited = true
    logDebug(s"ForwardPushToPollingRespSO.ensureInited called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onNext(resp: Proxy.Metadata): Unit = {
    logTrace(s"ForwardPushToPollingRespSO.onNext calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

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

    logTrace(s"ForwardPushToPollingRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"ForwardPushToPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", wrapped)
    finishLatch.countDown()
    pollingResults.setError(wrapped)
    logError(s"ForwardPushToPollingRespSO.onError called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onCompleted(): Unit = {
    logTrace(s"ForwardPollingToPushRespSO.onCompleted calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
    finishLatch.countDown()
    pollingResults.setFinish()
    logTrace(s"ForwardPollingToPushRespSO.onCompleted called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
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
    if (inited) return
    logTrace(s"UnaryCallPollingRespSO.ensureInited calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")

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
      logTrace(s"UnaryCallPollingRespSO.onNext calling. do stub.unaryCall starting. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
      val callResult = stub.unaryCall(req.getPacket)
      logTrace(s"UnaryCallPollingRespSO.onNext calling. do stub.unaryCall finished. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
      pollingFrameSeq += 1
      val response = Proxy.PollingFrame.newBuilder()
        .setMethod(PollingMethods.UNARY_CALL)
        .setPacket(callResult)
        .setSeq(pollingFrameSeq)
        .build()
      logTrace(s"UnaryCallPollingRespSO.onNext calling. do pollingResults.put. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
      pollingResults.put(response)
      pollingResults.setFinish()
    } catch {
      case t:Throwable =>
        onError(t)
    }

    logTrace(s"UnaryCallPollingRespSO.onNext called. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}")
  }

  override def onError(t: Throwable): Unit = {
    val wrapped = TransferExceptionUtils.throwableToException(t)
    logError(s"UnaryCallPollingRespSO.onError calling. rsKey=${rsKey}, rsHeader=${rsHeader}, metadata=${oneLineStringMetadata}", t)
    pollingResults.setError(wrapped)
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
    if (PollingMethods.ERROR_POISON.equals(req.getMethod)) {
      logError(req.getDesc)
    }
    pollingRespSO.onNext(Proxy.PollingFrame.newBuilder().setMethod("mock").setSeq(12399l).build())

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
    // pollingResults.put(v)
  }

  override def onError(throwable: Throwable): Unit = {
    //pollingResults.setError(throwable)
    logError(throwable)
  }

  override def onCompleted(): Unit = {
    logInfo("complete")
  }
}