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

package com.webank.eggroll.core.grpc.client;

import com.google.protobuf.Message;
import com.webank.eggroll.core.concurrent.AwaitSettableFuture;
import com.webank.eggroll.core.error.handler.DefaultLoggingErrorHandler;
import com.webank.eggroll.core.error.handler.ErrorHandler;
import com.webank.eggroll.core.error.handler.InterruptAndRethrowRuntimeErrorHandler;
import com.webank.eggroll.core.factory.GrpcStreamComponentFactory;
import com.webank.eggroll.core.util.ErrorUtils;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * S: Stub type R: calleR type E: calleE type
 */
public class GrpcClientTemplate<S extends AbstractStub, R extends Message, E extends Message> {

  private static final Logger LOGGER = LogManager.getLogger();
  private GrpcStreamComponentFactory grpcStreamComponentFactory;
  private GrpcAsyncClientContext<S, R, E> grpcAsyncClientContext;
  private StreamProcessor<R> streamProcessor;
  private StreamObserver<R> requestObserver;
  private ErrorHandler loggingErrorHandler = new DefaultLoggingErrorHandler();
  private ErrorHandler rethrowErrorHandler = new InterruptAndRethrowRuntimeErrorHandler();

  public GrpcClientTemplate<S, R, E> setGrpcAsyncClientContext(
      GrpcAsyncClientContext<S, R, E> grpcAsyncClientContext) {
    this.grpcAsyncClientContext = grpcAsyncClientContext;
    return this;
  }

  public void initCallerStreamingRpc() {
    S stub = grpcAsyncClientContext.createStub();

    CountDownLatch finishLatch = grpcAsyncClientContext.createFinishLatch();

    @SuppressWarnings("unchecked")
    StreamObserver<E> responseObserver
        = (StreamObserver<E>) grpcStreamComponentFactory.createCallerResponseStreamObserver(
        grpcAsyncClientContext.getCallerStreamObserverClass(),
        finishLatch,
        grpcAsyncClientContext.getStreamObserverInitArgs());

    requestObserver = grpcAsyncClientContext.getCallerStreamingMethodInvoker()
        .invoke(stub, responseObserver);

    streamProcessor = (StreamProcessor<R>) grpcStreamComponentFactory
        .createStreamProcessor(grpcAsyncClientContext.getRequestStreamProcessorClass(),
            requestObserver,
            grpcAsyncClientContext.getRequestStreamProcessorInitArgs());

    streamProcessor.onInit();
  }

  public void processCallerStreamingRpc() {
    streamProcessor.onProcess();
  }

  public void errorCallerStreamingRpc(Throwable t) {
    requestObserver.onError(ErrorUtils.toGrpcRuntimeException(t));
  }

  public void completeStreamingRpc() {
    streamProcessor.onComplete();
    if (!(streamProcessor instanceof BaseClientCallStreamProcessor)) {
      try {
        requestObserver.onCompleted();
      } catch (IllegalStateException e) {
        loggingErrorHandler.handleError(ErrorUtils.toGrpcRuntimeException(e));
      }
    }

    grpcAsyncClientContext.awaitFinish(grpcAsyncClientContext.getAttemptTimeout(),
        grpcAsyncClientContext.getAttemptTimeoutUnit(),
        grpcAsyncClientContext.getErrorHandler());
  }

  public void calleeStreamingRpc(R request) {
    S stub = grpcAsyncClientContext.createStub();

    CountDownLatch finishLatch = grpcAsyncClientContext.createFinishLatch();

    @SuppressWarnings("unchecked")
    StreamObserver<E> responseObserver
        = (StreamObserver<E>) grpcStreamComponentFactory.createCallerResponseStreamObserver(
        grpcAsyncClientContext.getCallerStreamObserverClass(),
        finishLatch,
        grpcAsyncClientContext.getStreamObserverInitArgs());

    grpcAsyncClientContext.getCalleeStreamingMethodInvoker()
        .invoke(stub, request, responseObserver);

    grpcAsyncClientContext.awaitFinish(grpcAsyncClientContext.getAttemptTimeout(),
        grpcAsyncClientContext.getAttemptTimeoutUnit(),
        grpcAsyncClientContext.getErrorHandler());

    try {
      responseObserver.onCompleted();
    } catch (IllegalStateException ignore) {
      LOGGER.warn("warning in grpc call", ignore);
    }
  }

  public <T> T calleeStreamingRpcWithImmediateDelayedResult(R request,
      AwaitSettableFuture<T> delayedResult)
      throws ExecutionException {
    calleeStreamingRpc(request);

    if (delayedResult.hasError()) {
      throw new ExecutionException(delayedResult.getError());
    }

    T result = delayedResult.getNow();
    return result;
  }

  public <T> T calleeStreamingRpcWithTimeoutDelayedResult(R request,
      AwaitSettableFuture<T> delayedResult,
      long timeout, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    calleeStreamingRpc(request);

    if (delayedResult.hasError()) {
      throw new ExecutionException(delayedResult.getError());
    }

    T result = null;

    result = delayedResult.get(timeout, timeUnit);

    return result;
  }
}
