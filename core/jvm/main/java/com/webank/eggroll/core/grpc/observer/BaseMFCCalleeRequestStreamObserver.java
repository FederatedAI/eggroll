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

import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base callee request stream observer with Manual Flow Control (MFC) aware
 *
 * @param <R> calleR type
 * @param <E> calleE type
 */
public abstract class BaseMFCCalleeRequestStreamObserver<R, E> extends
    BaseCalleeRequestStreamObserver<R, E> {

  private final AtomicBoolean wasReady;
  private final ServerCallStreamObserver<E> serverCallStreamObserver;

  public BaseMFCCalleeRequestStreamObserver(final ServerCallStreamObserver<E> callerNotifier,
      final AtomicBoolean wasReady) {
    super(callerNotifier);
    this.wasReady = wasReady;
    this.serverCallStreamObserver = callerNotifier;
  }

  @Override
  public void onNext(R value) {
    if (serverCallStreamObserver.isReady()) {
      serverCallStreamObserver.request(1);
    } else {
      wasReady.set(false);
    }
  }
}
