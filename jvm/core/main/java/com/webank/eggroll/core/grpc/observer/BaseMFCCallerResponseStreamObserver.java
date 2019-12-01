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
import com.webank.eggroll.core.grpc.processor.StreamProcessor;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.concurrent.CountDownLatch;

/**
 * Base caller response stream observer with Manual Flow Control (MFC) aware
 *
 * @param <R> calleR type
 * @param <E> calleE type
 */
public class BaseMFCCallerResponseStreamObserver<R extends Message, E extends Message> extends
    BaseCallerResponseStreamObserver<R, E> implements
    ClientResponseObserver<R, E> {

  private ClientCallStreamObserver<R> requestStream;
  private StreamProcessor<R> streamProcessor;

  public BaseMFCCallerResponseStreamObserver(CountDownLatch finishLatch,
      StreamProcessor<R> streamProcessor) {
    super(finishLatch);
    this.streamProcessor = streamProcessor;
  }


  @Override
  public void beforeStart(ClientCallStreamObserver<R> requestStream) {
    this.requestStream = requestStream;

    requestStream.disableAutoInboundFlowControl();

    requestStream.setOnReadyHandler(() -> {
      if (requestStream.isReady()) {
        streamProcessor.notifyReady();
      }
    });
  }

  @Override
  public void onNext(E e) {
    requestStream.request(1);
  }
}
