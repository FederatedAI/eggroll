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

package com.webank.eggroll.core.grpc.processor;

import com.webank.eggroll.core.error.handler.ErrorHandler;
import com.webank.eggroll.core.error.handler.InterruptAndRethrowRuntimeErrorHandler;
import io.grpc.stub.ClientCallStreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class BaseClientCallStreamProcessor<R> implements StreamProcessor<R> {

  protected ClientCallStreamObserver<R> clientCallStreamObserver;
  private Lock conditionLock;
  protected Condition streamReady;
  protected ErrorHandler errorHandler = new InterruptAndRethrowRuntimeErrorHandler();
  protected AtomicInteger stage = new AtomicInteger(0);

  private final Logger LOGGER = LogManager.getLogger(this.getClass());

  public BaseClientCallStreamProcessor(ClientCallStreamObserver<R> clientCallStreamObserver) {
    this.clientCallStreamObserver = clientCallStreamObserver;
    this.conditionLock = new ReentrantLock();
    this.streamReady = conditionLock.newCondition();
  }

  @Override
  public void onInit() {
    if (stage.get() > 0) {
      return;
    }

  }

  @Override
  public void onProcess() {
    if (stage.get() == 0) {
      onInit();
    }

    try {
      if (clientCallStreamObserver.isReady()) {
        return;
      }

      // todo:2: bind to configuration
      boolean awaitResult = streamReady.await(10, TimeUnit.MINUTES);
      if (!awaitResult && !clientCallStreamObserver.isReady()) {
        throw new TimeoutException("stream processor await timeout");
      }
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(Throwable t) {
    errorHandler.handleError(t);
  }

  @Override
  public void onComplete() {
    try {
      clientCallStreamObserver.onCompleted();
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void notifyReady() {
    conditionLock.lock();
    try {
      streamReady.signalAll();
    } finally {
      conditionLock.unlock();
    }
  }
}
