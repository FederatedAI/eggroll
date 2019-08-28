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
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.di.Singletons;
import com.webank.eggroll.core.error.handler.ErrorHandler;
import com.webank.eggroll.core.error.handler.InterruptAndRethrowRuntimeErrorHandler;
import com.webank.eggroll.core.factory.GrpcStubFactory;
import com.webank.eggroll.core.grpc.observer.BaseCallerResponseStreamObserver;
import com.webank.eggroll.core.model.Endpoint;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import java.lang.reflect.ParameterizedType;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Client contexts containing all information to make a gRPC call
 * <p>
 * S: Stub type R: calleR type E: calleE type
 */
public class GrpcAsyncClientContext<S extends AbstractStub, R extends Message, E extends Message> {

  private static final Logger LOGGER = LogManager.getLogger();

  // todo: replace with Injection
  private GrpcStubFactory grpcStubFactory = Singletons.getNoCheck(GrpcStubFactory.class);
  private ErrorHandler errorHandler = Singletons
      .getNoCheck(InterruptAndRethrowRuntimeErrorHandler.class);
  private Class<? extends BaseCallerResponseStreamObserver> callerStreamObserverClass;
  private GrpcCallerStreamingStubMethodInvoker<S, R, E> callerStreamingMethodInvoker;
  private GrpcCalleeStreamingStubMethodInvoker<S, R, E> calleeStreamingMethodInvoker;
  private StreamProcessor<R> requestStreamProcessor;
  private long attemptTimeout;
  private TimeUnit attemptTimeoutUnit;
  private Object[] streamObserverInitArgs;
  private Class<? extends StreamProcessor> requestStreamProcessorClass;
  private Object[] requestStreamProcessorInitArgs;
  private S stub;
  private Class<? extends AbstractStub> stubClass;
  private Class<?> grpcClass;
  private Metadata grpcMetadata;
  private Endpoint serverEndpoint;
  private int latchInitCount = 1;
  private boolean isSecureRequest;
  private CountDownLatch finishLatch;

  public void init() {
    if (stubClass == null) {
      this.stubClass = (Class<S>) ((ParameterizedType) getClass().getGenericSuperclass())
          .getActualTypeArguments()[0];
    }

    try {
      this.grpcClass = Class.forName(
          StringUtils.substringBeforeLast(stubClass.getCanonicalName(), StringConstants.DOT()));
    } catch (ClassNotFoundException e) {
      Thread.currentThread().interrupt();
      throw new IllegalArgumentException(e);
    }
  }

  public ErrorHandler getErrorHandler() {
    return errorHandler;
  }

  public GrpcAsyncClientContext<S, R, E> setErrorHandler(
      ErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
    return this;
  }

  public Class<? extends BaseCallerResponseStreamObserver> getCallerStreamObserverClass() {
    return callerStreamObserverClass;
  }

  public GrpcAsyncClientContext<S, R, E> setCallerStreamObserverClass(
      Class<? extends BaseCallerResponseStreamObserver> callerStreamObserverClass) {
    this.callerStreamObserverClass = callerStreamObserverClass;
    return this;
  }

  public GrpcCallerStreamingStubMethodInvoker<S, R, E> getCallerStreamingMethodInvoker() {
    return callerStreamingMethodInvoker;
  }

  public GrpcAsyncClientContext<S, R, E> setCallerStreamingMethodInvoker(
      GrpcCallerStreamingStubMethodInvoker<S, R, E> callerStreamingMethodInvoker) {
    this.callerStreamingMethodInvoker = callerStreamingMethodInvoker;
    return this;
  }

  public GrpcCalleeStreamingStubMethodInvoker<S, R, E> getCalleeStreamingMethodInvoker() {
    return calleeStreamingMethodInvoker;
  }

  public GrpcAsyncClientContext<S, R, E> setCalleeStreamingMethodInvoker(
      GrpcCalleeStreamingStubMethodInvoker<S, R, E> calleeStreamingMethodInvoker) {
    this.calleeStreamingMethodInvoker = calleeStreamingMethodInvoker;
    return this;
  }

  public StreamProcessor<R> getRequestStreamProcessor() {
    return requestStreamProcessor;
  }

  public GrpcAsyncClientContext<S, R, E> setRequestStreamProcessor(
      StreamProcessor<R> requestStreamProcessor) {
    this.requestStreamProcessor = requestStreamProcessor;
    return this;
  }

  public long getAttemptTimeout() {
    return attemptTimeout;
  }

  public GrpcAsyncClientContext<S, R, E> setAttemptTimeout(long attemptTimeout) {
    this.attemptTimeout = attemptTimeout;
    return this;
  }

  public TimeUnit getAttemptTimeoutUnit() {
    return attemptTimeoutUnit;
  }

  public GrpcAsyncClientContext<S, R, E> setAttemptTimeoutUnit(
      TimeUnit attemptTimeoutUnit) {
    this.attemptTimeoutUnit = attemptTimeoutUnit;
    return this;
  }

  public Object[] getStreamObserverInitArgs() {
    return streamObserverInitArgs;
  }

  public GrpcAsyncClientContext<S, R, E> setStreamObserverInitArgs(
      Object[] streamObserverInitArgs) {
    this.streamObserverInitArgs = streamObserverInitArgs;
    return this;
  }

  public Class<? extends StreamProcessor> getRequestStreamProcessorClass() {
    return requestStreamProcessorClass;
  }

  public GrpcAsyncClientContext<S, R, E> setRequestStreamProcessorClass(
      Class<? extends StreamProcessor> requestStreamProcessorClass) {
    this.requestStreamProcessorClass = requestStreamProcessorClass;
    return this;
  }

  public Object[] getRequestStreamProcessorInitArgs() {
    return requestStreamProcessorInitArgs;
  }

  public GrpcAsyncClientContext<S, R, E> setRequestStreamProcessorInitArgs(
      Object[] requestStreamProcessorInitArgs) {
    this.requestStreamProcessorInitArgs = requestStreamProcessorInitArgs;
    return this;
  }

  public S getStub() {
    return stub;
  }

  public GrpcAsyncClientContext<S, R, E> setStub(S stub) {
    this.stub = stub;
    return this;
  }

  public Class<? extends AbstractStub> getStubClass() {
    return stubClass;
  }

  public GrpcAsyncClientContext<S, R, E> setStubClass(
      Class<? extends AbstractStub> stubClass) {
    this.stubClass = stubClass;
    return this;
  }

  public Class<?> getGrpcClass() {
    return grpcClass;
  }

  public GrpcAsyncClientContext<S, R, E> setGrpcClass(Class<?> grpcClass) {
    this.grpcClass = grpcClass;
    return this;
  }

  public Metadata getGrpcMetadata() {
    return grpcMetadata;
  }

  public GrpcAsyncClientContext<S, R, E> setGrpcMetadata(Metadata grpcMetadata) {
    this.grpcMetadata = grpcMetadata;
    return this;
  }

  public Endpoint getServerEndpoint() {
    return serverEndpoint;
  }

  public GrpcAsyncClientContext<S, R, E> setServerEndpoint(
      Endpoint serverEndpoint) {
    this.serverEndpoint = serverEndpoint;
    return this;
  }

  public int getLatchInitCount() {
    return latchInitCount;
  }

  public GrpcAsyncClientContext<S, R, E> setLatchInitCount(int latchInitCount) {
    this.latchInitCount = latchInitCount;
    return this;
  }

  public boolean isSecureRequest() {
    return isSecureRequest;
  }

  public GrpcAsyncClientContext<S, R, E> setSecureRequest(boolean secureRequest) {
    isSecureRequest = secureRequest;
    return this;
  }

  public GrpcAsyncClientContext<S, R, E> setCallerStreamObserverClassAndInitArgs(
      Class<? extends BaseCallerResponseStreamObserver> callerStreamObserverClass,
      Object... specificInitArgs) {
    this.callerStreamObserverClass = callerStreamObserverClass;
    this.streamObserverInitArgs = specificInitArgs;
    return this;
  }

  public GrpcAsyncClientContext<S, R, E> setRequestStreamProcessorClassAndArgs(
      Class<? extends StreamProcessor<R>> streamProcessorClass,
      Object... constructorArgs) {
    this.requestStreamProcessorClass = streamProcessorClass;
    this.requestStreamProcessorInitArgs = constructorArgs;
    return this;
  }

  public GrpcAsyncClientContext<S, R, E> setAttemptTimeout(long attemptTimeout,
      TimeUnit attemptTimeoutUnit) {
    this.attemptTimeout = attemptTimeout;
    this.attemptTimeoutUnit = attemptTimeoutUnit;
    return this;
  }

  public S createStub() {
    if (stub == null) {
      init();

      stub = (S) grpcStubFactory.createGrpcStub(true, grpcClass, serverEndpoint, isSecureRequest);
      if (grpcMetadata != null) {
        stub = (S) MetadataUtils.attachHeaders(stub, grpcMetadata);
      }
    }

    return stub;
  }

  public CountDownLatch createFinishLatch() {
    this.finishLatch = new CountDownLatch(latchInitCount);
    return finishLatch;
  }

  public boolean awaitFinish() throws InterruptedException {
    return awaitFinish(attemptTimeout, attemptTimeoutUnit);
  }

  public boolean awaitFinish(long timeout, TimeUnit unit) throws InterruptedException {
    return this.finishLatch.await(timeout, unit);
  }

  public boolean awaitFinish(long timeout, TimeUnit unit, ErrorHandler errorHandler) {
    if (errorHandler == null) {
      errorHandler = this.errorHandler;
    }

    boolean result = false;

    try {
      result = awaitFinish(timeout, unit);
    } catch (InterruptedException e) {
      errorHandler.handleError(e);
      Thread.currentThread().interrupt();
    }

    return result;
  }

}
