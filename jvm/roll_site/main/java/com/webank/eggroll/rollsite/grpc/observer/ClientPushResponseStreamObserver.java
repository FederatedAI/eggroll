/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
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

package com.webank.eggroll.rollsite.grpc.observer;

import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.infra.ResultCallback;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class ClientPushResponseStreamObserver implements StreamObserver<Proxy.Metadata> {
    private static final Logger LOGGER = LogManager.getLogger(ClientPushResponseStreamObserver.class);
    private final CountDownLatch finishLatch;
    private final ResultCallback<Metadata> resultCallback;
    private Proxy.Metadata metadata;

    public ClientPushResponseStreamObserver(ResultCallback<Proxy.Metadata> resultCallback, CountDownLatch finishLatch) {
        this.resultCallback = resultCallback;
        this.finishLatch = finishLatch;
    }

    @Override
    public void onNext(Proxy.Metadata metadata) {
        this.metadata = metadata;
        resultCallback.setResult(metadata);
        LOGGER.trace("[PUSH][CLIENTOBSERVER][ONNEXT] ClientPushResponseStreamObserver.onNext(), metadata: {}",
                ToStringUtils.toOneLineString(metadata));
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("[PUSH][CLIENTOBSERVER][ONERROR] error in push client, metadata: {}", ToStringUtils.toOneLineString(metadata));
        LOGGER.error(ExceptionUtils.getStackTrace(throwable));
        finishLatch.countDown();
    }

    @Override
    public void onCompleted() {
        LOGGER.trace("[PUSH][CLIENTOBSERVER][ONCOMPLETE] ClientPushResponseStreamObserver.onCompleted(), metadata: {}",
            ToStringUtils.toOneLineString(metadata));
        finishLatch.countDown();
    }
}
