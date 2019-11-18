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

package com.webank.eggroll.core.command

import java.util.concurrent.CountDownLatch

import com.webank.eggroll.core.command.Command.CommandResponse
import com.webank.eggroll.core.command.CommandModelPbMessageSerdes._
import com.webank.eggroll.core.concurrent.AwaitSettableFuture
import com.webank.eggroll.core.constant.SerdesTypes
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.grpc.client.{GrpcClientContext, GrpcClientTemplate}
import com.webank.eggroll.core.grpc.observer.SameTypeFutureCallerResponseStreamObserver
import com.webank.eggroll.core.meta.MetaModelPbMessageSerdes._
import com.webank.eggroll.core.meta.{ErEndpoint, ErTask}
import com.webank.eggroll.core.util.{Logging, SerdesUtils}
import io.grpc.stub.StreamObserver

class CommandClient() extends Logging {
  def sendTask(task: ErTask, command: CommandURI): ErCommandResponse = {
    val delayedResult = new AwaitSettableFuture[CommandResponse]

    val context = new GrpcClientContext[
      CommandServiceGrpc.CommandServiceStub,
      Command.CommandRequest,
      CommandResponse]()

    context.setServerEndpoint(task.getCommandEndpoint)
      .setCalleeStreamingMethodInvoker(
        (stub: CommandServiceGrpc.CommandServiceStub,
         request: Command.CommandRequest,
         responseObserver: StreamObserver[CommandResponse])
        => stub.call(request, responseObserver))
      .setCallerStreamObserverClassAndInitArgs(classOf[CommandResponseObserver], delayedResult)
      .setStubClass(classOf[CommandServiceGrpc.CommandServiceStub])

    val template = new GrpcClientTemplate[
      CommandServiceGrpc.CommandServiceStub,
      Command.CommandRequest,
      CommandResponse]()
      .setGrpcClientContext(context)

    val request = ErCommandRequest(id = "100", uri = command.uri.toString, args = Array(task.toProto().toByteArray))

    val result = template.calleeStreamingRpcWithImmediateDelayedResult(request.toProto(), delayedResult)

    result.fromProto()
  }

  def syncSend[T >: RpcMessage](input: RpcMessage,
                                outputType: Class[T],
                                endpoint: ErEndpoint,
                                commandUri: CommandURI,
                                serdesTypes: String = SerdesTypes.PROTOBUF): T = {
    val delayedResult = new AwaitSettableFuture[CommandResponse]

    val context = new GrpcClientContext[
      CommandServiceGrpc.CommandServiceStub,
      Command.CommandRequest,
      CommandResponse]()

    context.setServerEndpoint(endpoint)
      .setCalleeStreamingMethodInvoker(
        (stub: CommandServiceGrpc.CommandServiceStub,
         request: Command.CommandRequest,
         responseObserver: StreamObserver[CommandResponse])
        => stub.call(request, responseObserver))
      .setCallerStreamObserverClassAndInitArgs(classOf[CommandResponseObserver], delayedResult)
      .setStubClass(classOf[CommandServiceGrpc.CommandServiceStub])

    val template = new GrpcClientTemplate[
      CommandServiceGrpc.CommandServiceStub,
      Command.CommandRequest,
      CommandResponse]()
      .setGrpcClientContext(context)

    val request = ErCommandRequest(
      uri = commandUri.uri.toString, args = Array(SerdesUtils.rpcMessageToBytes(input)))

    val response = template.calleeStreamingRpcWithImmediateDelayedResult(
      request.toProto(), delayedResult)

    val erResponse = response.fromProto()

    val byteResult = erResponse.results(0)

    if (byteResult.length != 0)
      SerdesUtils.rpcMessageFromBytes(bytes = byteResult, targetType = outputType, serdesTypes = serdesTypes)
    else
      null
  }
}

class CommandResponseObserver(finishLatch: CountDownLatch, asFuture: AwaitSettableFuture[CommandResponse])
  extends SameTypeFutureCallerResponseStreamObserver[Command.CommandRequest, CommandResponse](finishLatch, asFuture) {
}