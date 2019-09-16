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

import com.webank.eggroll.core.grpc.observer.BaseCallerResponseStreamObserver;
import com.webank.eggroll.grpc.test.GrpcTest.HelloRequest;
import com.webank.eggroll.grpc.test.GrpcTest.HelloResponse;
import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HelloCallerResponseStreamObserver extends
    BaseCallerResponseStreamObserver<HelloRequest, HelloResponse> {

  private final Logger LOGGER = LogManager.getLogger();

  public HelloCallerResponseStreamObserver(CountDownLatch finishLatch) {
    super(finishLatch);
  }

  /**
   * Receives a value from the stream.
   *
   * <p>Can be called many times but is never called after {@link #onError(Throwable)} or {@link
   * #onCompleted()} are called.
   *
   * <p>Unary calls must invoke onNext at most once.  Clients may invoke onNext at most once for
   * server streaming calls, but may receive many onNext callbacks.  Servers may invoke onNext at
   * most once for client streaming calls, but may receive many onNext callbacks.
   *
   * <p>If an exception is thrown by an implementation the caller is expected to terminate the
   * stream by calling {@link #onError(Throwable)} with the caught exception prior to propagating
   * it.
   *
   * @param value the value passed to the stream
   */
  @Override
  public void onNext(HelloResponse value) {
    LOGGER.info("response received: {}", value.getMsg());
  }
}
