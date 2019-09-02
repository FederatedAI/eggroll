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

import com.webank.eggroll.core.grpc.processor.BaseClientCallStreamProcessor;
import com.webank.eggroll.grpc.test.GrpcTest.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import java.util.ArrayList;
import java.util.Iterator;

public class HelloPushStreamProcessor extends BaseClientCallStreamProcessor<HelloRequest> {

  private ArrayList<String> msgs;
  private Iterator<String> iter;
  private volatile boolean isDone;
  private HelloRequest.Builder builder;

  public HelloPushStreamProcessor(ClientCallStreamObserver<HelloRequest> clientCallStreamObserver,
      ArrayList<String> msgs) {
    super(clientCallStreamObserver);
    this.msgs = msgs;
    this.iter = msgs.iterator();
    this.isDone = false;
    this.builder = HelloRequest.newBuilder();
  }

  @Override
  public void onProcess() {
    if (!isDone && iter.hasNext()) {
      HelloRequest request = builder.setMsg(iter.next()).build();
      clientCallStreamObserver.onNext(request);

      if (!iter.hasNext()) {
        isDone = true;
      }
    }
  }
}
