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

package com.webank.eggroll.core.testgrpc;

import com.webank.eggroll.core.grpc.server.GrpcServerCallable;
import com.webank.eggroll.core.grpc.server.GrpcServerWrapper;
import com.webank.eggroll.grpc.test.GrpcTest.HelloRequest;
import com.webank.eggroll.grpc.test.GrpcTest.HelloResponse;
import com.webank.eggroll.grpc.test.HelloServiceGrpc.HelloServiceImplBase;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HelloServer extends HelloServiceImplBase {

  private GrpcServerWrapper grpcServerWrapper = new GrpcServerWrapper();

  private static final Logger LOGGER = LogManager.getLogger();

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void unaryCall(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
    grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
      LOGGER.info(request);
      HelloResponse response = HelloResponse.newBuilder().setMsg("hello " + request.getMsg())
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    });
  }

  /**
   * @param responseObserver
   */
  @Override
  public StreamObserver<HelloRequest> push(StreamObserver<HelloResponse> responseObserver) {
    return grpcServerWrapper.wrapGrpcServerCallable(responseObserver,
        (GrpcServerCallable<StreamObserver<HelloRequest>>) () -> {
          LOGGER.info("push");

          final ServerCallStreamObserver<HelloResponse> serverCallStreamObserver
              = (ServerCallStreamObserver<HelloResponse>) responseObserver;

          serverCallStreamObserver.disableAutoInboundFlowControl();

          final AtomicBoolean wasReady = new AtomicBoolean(false);

          serverCallStreamObserver.setOnReadyHandler(() -> {
            if (serverCallStreamObserver.isReady() && wasReady.compareAndSet(false, true)) {
              serverCallStreamObserver.request(1);
            }
          });

          HelloPushCalleeRequestStreamObserver result = new HelloPushCalleeRequestStreamObserver(
              serverCallStreamObserver, wasReady);
          return result;
        });
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void pull(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
    super.pull(request, responseObserver);
  }
}
