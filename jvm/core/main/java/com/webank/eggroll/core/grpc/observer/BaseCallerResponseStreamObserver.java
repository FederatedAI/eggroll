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

package com.webank.eggroll.core.grpc.observer;

import com.google.protobuf.Message;
import com.webank.eggroll.core.util.ErrorUtils;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is used at CLIENT SIDE in source streaming for server to handleCheckedException
 * incoming stream objects.
 *
 * @param <R> calleR parameter type (in the source streaming context, objects from type S are being
 *            streaming out)
 * @param <E> calleE parameter type (in the source streaming context, an object from type R will be
 *            returned)
 */
public abstract class BaseCallerResponseStreamObserver<R extends Message, E extends Message>
    implements StreamObserver<E> {

  private final CountDownLatch finishLatch;
  private final Logger LOGGER = LogManager.getLogger(this);

  private Throwable throwable;
  private String classSimpleName;

  public BaseCallerResponseStreamObserver(CountDownLatch finishLatch) {
    this.finishLatch = finishLatch;
    this.classSimpleName = this.getClass().getSimpleName();
  }

  @Override
  public void onError(Throwable throwable) {
    finishLatch.countDown();
    throwable = ErrorUtils.toGrpcRuntimeException(throwable);
    if (LOGGER.isErrorEnabled()) {
      LOGGER.error(classSimpleName + " source streaming error", throwable);
    }
  }

  @Override
  public void onCompleted() {
    finishLatch.countDown();
  }
}
