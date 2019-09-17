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
 */

package com.webank.eggroll.core.transfer

import com.webank.eggroll.core.concurrent.AwaitSettableFuture
import com.webank.eggroll.core.grpc.client.{GrpcClientContext, GrpcClientTemplate}
import com.webank.eggroll.core.grpc.observer.SameTypeCallerResponseStreamObserver
import com.webank.eggroll.core.grpc.processor.BaseClientCallStreamProcessor
import com.webank.eggroll.core.meta.TransferModelPbSerdes._
import com.webank.eggroll.core.meta.{ErBatch, ErServerNode, ErTransferHeader}
import io.grpc.stub.{ClientCallStreamObserver, StreamObserver}

class TransferClient {
  def send(data: Array[Byte], tag: String, serverNode: ErServerNode): Unit = {
    val context = new GrpcClientContext[TransferServiceGrpc.TransferServiceStub, Transfer.Batch, Transfer.TransferHeader]

    val delayedResult = new AwaitSettableFuture[Transfer.TransferHeader]

    context.setStubClass(classOf[TransferServiceGrpc.TransferServiceStub])
      .setServerEndpoint(serverNode.endpoint)
      .setCallerStreamingMethodInvoker((stub: TransferServiceGrpc.TransferServiceStub,
                                        responseObserver: StreamObserver[Transfer.TransferHeader]) => stub.send(responseObserver))
      .setCallerStreamObserverClassAndInitArgs(classOf[SameTypeCallerResponseStreamObserver[Transfer.Batch, Transfer.TransferHeader]])
      .setRequestStreamProcessorClassAndArgs(classOf[TransferSendStreamProcessor], data, tag)

    val template = new GrpcClientTemplate[TransferServiceGrpc.TransferServiceStub, Transfer.Batch, Transfer.TransferHeader]

    template.setGrpcClientContext(context)

    template.initCallerStreamingRpc()
    template.processCallerStreamingRpc()
    template.completeStreamingRpc()
  }
}

class TransferSendStreamProcessor(clientCallStreamObserver: ClientCallStreamObserver[Transfer.Batch], data: Array[Byte], tag: String)
  extends BaseClientCallStreamProcessor[Transfer.Batch](clientCallStreamObserver) {
  override def onProcess(): Unit = {
    val batch = ErBatch(header = ErTransferHeader(id = 100, tag = this.tag, totalSize = data.size), data = this.data)
    clientCallStreamObserver.onNext(batch.toProto())
  }
}