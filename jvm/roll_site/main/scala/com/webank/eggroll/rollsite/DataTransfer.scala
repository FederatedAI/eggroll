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

import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.meta.{ErEndpoint, ErRollSiteHeader}
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.util.{ErrorUtils, Logging, ThreadPoolUtils, ToStringUtils}
import io.grpc.stub.StreamObserver

class DataTransferService extends DataTransferServiceGrpc.DataTransferServiceImplBase with Logging {
  private val packetBuilder = Proxy.Packet.newBuilder()
  private val headerBuilder = Proxy.Metadata.newBuilder()
  private val bodyBuilder = Proxy.Data.newBuilder()
  /**
   */
  override def push(responseObserver: StreamObserver[Proxy.Metadata]): StreamObserver[Proxy.Packet] = {
    new ProxyDispatchStreamObserver(responseObserver)
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
    logInfo(s"unary call received. ${ToStringUtils.toOneLineString(request)}")

    try {
      val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
      val dstPartyId = request.getHeader.getDst.getPartyId
      val result = if (myPartyId.equals(dstPartyId)) {
        logInfo(s"unary call: ${ToStringUtils.toOneLineString(request)}")
        request
        //processCommand(request)
      } else {
        val channel = GrpcClientUtils.getChannel(new ErEndpoint("localhost", 9470))
        val stub = DataTransferServiceGrpc.newBlockingStub(channel)

        stub.unaryCall(request)
      }

      responseSO.onNext(result)
      responseSO.onCompleted()
    } catch {
      case t: Throwable =>
        val wrapped = ErrorUtils.toGrpcRuntimeException(t)
        logError(wrapped)
        responseSO.onError(t)
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
    if (!DataTransferService.jobIdToSession.asMap().containsKey(jobId)) {
      val erSessionId = pairInfo.getDataKey
      val erSession = new ErSession(sessionId = erSessionId, createIfNotExists = false)

      DataTransferService.jobIdToSession.put(jobId, erSession)
    }

    request.toBuilder
      .setHeader(header.toBuilder.setAck(header.getSeq))
      .build()
  }
}

object DataTransferService {
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

class ProxyDispatchStreamObserver(responseSO: StreamObserver[Proxy.Metadata]) extends StreamObserver[Proxy.Packet] with Logging {
  private var proxySO: StreamObserver[Proxy.Packet] = _
  private var inited = false
  logInfo("358constructing proxy dispatcher")
  override def onNext(request: Proxy.Packet): Unit = {
    if (!inited) {
      val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
      val dstPartyId = request.getHeader.getDst.getPartyId
      logInfo("initing proxy dispatcher")

      proxySO = if (myPartyId.equals(dstPartyId)) {
        new SinkRequestStreamObserver(responseSO)
      } else {
        new ForwardRequestStreamObserver(responseSO)
      }
      inited = true
    }

    proxySO.onNext(request)
  }

  override def onError(throwable: Throwable): Unit = {
    proxySO.onError(throwable)
  }

  override def onCompleted(): Unit = {
    proxySO.onCompleted()
  }
}

class SinkRequestStreamObserver(respSO: StreamObserver[Proxy.Metadata])
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


class ForwardRequestStreamObserver(prevRespSO: StreamObserver[Proxy.Metadata])
  extends StreamObserver[Proxy.Packet] with Logging {
  private var inited = false
  private var nextReqSO: StreamObserver[Proxy.Packet] = _
  private val self = this

  override def onNext(request: Proxy.Packet): Unit = {
    logInfo(s"onnext: ${ToStringUtils.toOneLineString(request)}")

    if (!inited) {
      val channel = GrpcClientUtils.getChannel(new ErEndpoint("localhost", 9470))
      val stub = DataTransferServiceGrpc.newStub(channel)
      nextReqSO = stub.push(new StreamObserver[Proxy.Metadata] {
        override def onNext(value: Proxy.Metadata): Unit = {
          prevRespSO.onNext(value)
          logInfo("response received")
        }

        override def onError(t: Throwable): Unit = {
          self.onError(t)
        }

        override def onCompleted(): Unit = {
          logInfo("prevRespSO onComplete")
          prevRespSO.onCompleted()

          logInfo("finish forwarding")
        }
      })
      inited = true
    }

    nextReqSO.onNext(request)

    logInfo("forwarding")
  }

  override def onError(throwable: Throwable): Unit = {
    nextReqSO.onError(throwable)
    prevRespSO.onError(throwable)
  }

  override def onCompleted(): Unit = {
    logInfo("nextReqSO onComplete")
    nextReqSO.onCompleted()
  }
}