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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Future, ThreadPoolExecutor, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.meta.ErEndpoint
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
    // refer to ServerPushRequestStreamObserver, and simplifies it
    new StreamObserver[Proxy.Packet] {
      val inited = new AtomicBoolean(false)
      val broker = new FifoBroker[Proxy.Packet]()
      var processFunc: Proxy.Packet => Unit = _
      var response: Proxy.Metadata = _
      var client: DataTransferClient = _
      var clientFuture: Future[Proxy.Metadata] = _
      var isDst: Boolean = _

      override def onNext(request: Proxy.Packet): Unit = {
        /**
         * check if dst is myself:
         *   - yes -> gets a ErSession and sends data to it using internal protocol (the one which TransferService is using), but reserve the possibility to use external protocol (proxy protocol).
         *   - no -> forwards it to the next hop.
         */

        def whenDst(request: Proxy.Packet): Unit = {
          // use internal protocol to put all
          logInfo(s"got data in dst: ${ToStringUtils.toOneLineString(request)}")
        }

        def whenNotDst(request: Proxy.Packet): Unit = {
          broker.broker.put(request)
        }

        val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
        val dstPartyId = request.getHeader.getDst.getPartyId
        if (!inited.get()) {
          // get host:port from router
          processFunc = if (myPartyId.equals(dstPartyId)) {
            isDst = true
            logInfo("i am dst")
            clientFuture = DataTransferService.dataTransferExecutor.submit(() => {
              Proxy.Metadata.getDefaultInstance
            })
            whenDst
          } else {
            isDst = false
            logInfo("i am not dst")

            clientFuture = DataTransferService.dataTransferExecutor.submit(() => {
              client = new DataTransferClient(ErEndpoint("localhost", 9470))
              client.push(broker)
            })
            whenNotDst
          }

          response = request.getHeader
          inited.compareAndSet(false, true)
        }

        processFunc(request)
        logInfo(s"push on next: ${ToStringUtils.toOneLineString(request)}")
      }

      override def onError(throwable: Throwable): Unit = {
        // error logic here
        logError(throwable)
      }

      override def onCompleted(): Unit = {
        // complete logic here
        broker.signalWriteFinish()

        response = clientFuture.get()

        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logInfo("completed")
      }
    }
  }

  /**
   */
  override def unaryCall(request: Proxy.Packet,
                         responseObserver: StreamObserver[Proxy.Packet]): Unit = {
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
        processCommand(request)
      } else {
        val client = new DataTransferClient(ErEndpoint("localhost", 9470))
        client.unaryCall(request)
      }

      responseObserver.onNext(result)
      responseObserver.onCompleted()
    } catch {
      case t: Throwable =>
        val wrapped = ErrorUtils.toGrpcRuntimeException(t)
        logError(wrapped)
        responseObserver.onError(wrapped)
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
  val dataTransferExecutor: ThreadPoolExecutor =
    ThreadPoolUtils.newFixedThreadPool(10, "data-transfer")

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

class DataTransferClient(defaultEndpoint: ErEndpoint, isSecure: Boolean = false) extends Logging {

  def push(requests: Iterator[Proxy.Packet],
           endpoint: ErEndpoint = defaultEndpoint,
           options: Map[String, String] = Map.empty): Proxy.Metadata = {
    val channel = GrpcClientUtils.getChannel(endpoint, isSecure, options)
    val stub = DataTransferServiceGrpc.newStub(channel)
    var result: Proxy.Metadata = null
    val finishLatch = new CountDownLatch(1)

    val streamObserver = stub.push(new StreamObserver[Proxy.Metadata] {
      // define what to do when server responds
      override def onNext(v: Proxy.Metadata): Unit = {
        result = v
        logInfo(s"response metadata: ${result}")
      }

      // define what to do when server gives error - backward propagation
      override def onError(throwable: Throwable): Unit = {
        // process error here
        finishLatch.countDown()
      }

      // define what to do when server finishes
      override def onCompleted(): Unit = {
        // process finish here
        finishLatch.countDown()
      }
    })

    for (request <- requests) {
      streamObserver.onNext(request)
    }

    streamObserver.onCompleted()
    finishLatch.await()

    result
  }

  def unaryCall(request: Proxy.Packet,
                endpoint: ErEndpoint = defaultEndpoint,
                options: Map[String, String] = Map.empty): Proxy.Packet = {
    val channel = GrpcClientUtils.getChannel(endpoint, isSecure, options)
    val stub = DataTransferServiceGrpc.newBlockingStub(channel)

    stub.unaryCall(request)
  }
}