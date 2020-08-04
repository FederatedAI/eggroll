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

import com.webank.ai.eggroll.api.networking.proxy.{DataTransferServiceGrpc, Proxy}
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.transfer.GrpcClientUtils
import com.webank.eggroll.core.util.{ErrorUtils, Logging}
import io.grpc.stub.StreamObserver

class DataTransferService extends DataTransferServiceGrpc.DataTransferServiceImplBase with Logging {
  /**
   */
  override def push(responseObserver: StreamObserver[Proxy.Metadata]): StreamObserver[Proxy.Packet] = {
    // refer to ServerPushRequestStreamObserver, and simplifies it
    new StreamObserver[Proxy.Packet] {
      override def onNext(v: Proxy.Packet): Unit = {
        /**
         * check if dst is myself:
         *   - yes -> gets a ErSession and sends data to it using internal protocol (the one which TransferService is using), but reserve the possibility to use external protocol (proxy protocol).
         *   - no -> forwards it to the next hop.
         */


      }

      override def onError(throwable: Throwable): Unit = {
        // error logic here
      }

      override def onCompleted(): Unit = {
        // complete logic here
      }
    }
  }

  /**
   */
  override def unaryCall(request: Proxy.Packet, responseObserver: StreamObserver[Proxy.Packet]): Unit = {
    /**
     * Check if dst is myself.
     *   - yes -> check command to see what the request wants.
     *   - no -> forwards it to the next hop synchronously.
     */

    try {
      val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get()
      val dstPartyId = request.getHeader.getDst.getPartyId
      val result = if (myPartyId.equals(dstPartyId)) {
        processCommand(request)
      } else {
        val client = new DataTransferClient(ErEndpoint("next.hop", 9999))
        client.unaryCall(request)
      }

      responseObserver.onNext(result)
      responseObserver.onCompleted()
    } catch {
      case t: _ =>
        val wrapped = ErrorUtils.toGrpcRuntimeException(t)
        logError(wrapped)
        responseObserver.onError(wrapped)
    }
  }

  // processes commands e.g. getStatus, pushObj, pullObj etc.
  private def processCommand(request: Proxy.Packet): Proxy.Packet = ???
}

class DataTransferClient(defaultEndpoint: ErEndpoint, isSecure: Boolean = false) extends Logging {

  def push(requests: Iterator[Proxy.Packet],
           endpoint: ErEndpoint = defaultEndpoint,
           options: Map[String, String] = Map.empty): Proxy.Metadata = {
    val channel = GrpcClientUtils.getChannel(endpoint, isSecure, options)
    val stub = DataTransferServiceGrpc.newStub(channel)
    var result: Proxy.Metadata = null
    val streamObserver = stub.push(new StreamObserver[Proxy.Metadata] {
      // define what to do when server responds
      override def onNext(v: Proxy.Metadata): Unit = {
        result = v
      }

      // define what to do when server gives error - backward propagation
      override def onError(throwable: Throwable): Unit = ???

      // define what to do when server finishes
      override def onCompleted(): Unit = ???
    })


    for (request <- requests) {
      streamObserver.onNext(request)
    }

    streamObserver.onCompleted()

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