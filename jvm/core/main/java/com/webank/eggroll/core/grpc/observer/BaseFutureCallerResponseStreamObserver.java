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
import com.webank.eggroll.core.concurrent.AwaitSettableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * @param <R> calleR parameter type
 * @param <E> calleE parameter type
 * @param <F> AwaitSettableFuture parameter type
 */
public abstract class BaseFutureCallerResponseStreamObserver<R extends Message, E extends Message, F extends Message>
    extends BaseCallerResponseStreamObserver<R, E> {

  // need to be set in derived class
  protected AwaitSettableFuture<F> asFuture;

  public BaseFutureCallerResponseStreamObserver(CountDownLatch finishLatch,
      AwaitSettableFuture<F> asFuture) {
    super(finishLatch);
    this.asFuture = asFuture;
  }

  @Override
  public void onError(Throwable throwable) {
    asFuture.setError(throwable);
    super.onError(throwable);
  }
}
