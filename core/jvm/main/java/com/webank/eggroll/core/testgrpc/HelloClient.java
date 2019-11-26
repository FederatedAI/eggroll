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

package com.webank.eggroll.core.testgrpc;

import com.webank.eggroll.core.concurrent.AwaitSettableFuture;
import com.webank.eggroll.core.grpc.client.GrpcClientContext;
import com.webank.eggroll.core.grpc.client.GrpcClientTemplate;
import com.webank.eggroll.core.grpc.observer.SameTypeCallerResponseStreamObserver;
import com.webank.eggroll.core.grpc.observer.SameTypeFutureCallerResponseStreamObserver;
import com.webank.eggroll.grpc.test.GrpcTest.HelloRequest;
import com.webank.eggroll.grpc.test.GrpcTest.HelloResponse;
import com.webank.eggroll.grpc.test.HelloServiceGrpc.HelloServiceStub;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class HelloClient {

  public String unaryCall(String msg) {
    GrpcClientContext<HelloServiceStub, HelloRequest, HelloResponse> context
        = new GrpcClientContext<>();
    context.setStubClass(HelloServiceStub.class)
        .setServerEndpoint("localhost", 50000)
        .setCalleeStreamingMethodInvoker(HelloServiceStub::unaryCall)
        .setCallerStreamObserverClassAndInitArgs(HelloCallerResponseStreamObserver.class);

    GrpcClientTemplate<HelloServiceStub, HelloRequest, HelloResponse> template
        = new GrpcClientTemplate<>();
    template.setGrpcClientContext(context);

    AwaitSettableFuture<String> delayedResult = new AwaitSettableFuture<>();
    HelloRequest request = HelloRequest.newBuilder().setMsg("test hello | ").build();

    template.calleeStreamingRpc(request);
    return "";
  }

  public String unaryCallWithSameImmediateResult(String msg) throws ExecutionException {
    GrpcClientContext<HelloServiceStub, HelloRequest, HelloResponse> context
        = new GrpcClientContext<>();

    AwaitSettableFuture<HelloResponse> delayedResult = new AwaitSettableFuture<>();
    context.setStubClass(HelloServiceStub.class)
        .setServerEndpoint("localhost", 50000)
        .setCalleeStreamingMethodInvoker(HelloServiceStub::unaryCall)
        .setCallerStreamObserverClassAndInitArgs(SameTypeFutureCallerResponseStreamObserver.class,
            delayedResult);

    GrpcClientTemplate<HelloServiceStub, HelloRequest, HelloResponse> template
        = new GrpcClientTemplate<>();
    template.setGrpcClientContext(context);

    HelloRequest request = HelloRequest.newBuilder().setMsg("test hello | ").build();

    HelloResponse result = null;
    try {
      result = template
          .calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return result.getMsg();
  }

  public void push(List<String> messages) {
    GrpcClientContext<HelloServiceStub, HelloRequest, HelloResponse> context
        = new GrpcClientContext<>();

    AwaitSettableFuture<HelloResponse> delayedResult = new AwaitSettableFuture<>();
    context.setStubClass(HelloServiceStub.class);

    context.setStubClass(HelloServiceStub.class)
        .setServerEndpoint("localhost", 50000)
        .setCallerStreamingMethodInvoker(HelloServiceStub::push)
        .setCallerStreamObserverClassAndInitArgs(SameTypeCallerResponseStreamObserver.class)
        .setRequestStreamProcessorClassAndArgs(HelloPushStreamProcessor.class, messages);

    GrpcClientTemplate<HelloServiceStub, HelloRequest, HelloResponse> template
        = new GrpcClientTemplate<>();

    template.setGrpcClientContext(context);
    template.initCallerStreamingRpc();

    for (int i = 0; i < 2; ++i) {
      template.processCallerStreamingRpc();
    }

    template.completeStreamingRpc();
  }
}
