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

package com.webank.ai.eggroll.networking.proxy.factory;

import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.networking.proxy.grpc.observer.ClientPullResponseStreamObserver;
import com.webank.ai.eggroll.networking.proxy.grpc.observer.ClientPushResponseStreamObserver;
import com.webank.ai.eggroll.networking.proxy.grpc.observer.ClientUnaryCallResponseStreamObserver;
import com.webank.ai.eggroll.networking.proxy.grpc.observer.ServerPushRequestStreamObserver;
import com.webank.ai.eggroll.networking.proxy.infra.Pipe;
import com.webank.ai.eggroll.networking.proxy.infra.ResultCallback;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class ProxyGrpcStreamObserverFactory {
    private static final Logger LOGGER = LogManager.getLogger(ProxyGrpcStreamObserverFactory.class);
    @Autowired
    private LocalBeanFactory localBeanFactory;
    @Autowired
    private ToStringUtils toStringUtils;

    public ClientPullResponseStreamObserver createClientPullResponseStreamObserver(Pipe pipe,
                                                                                   CountDownLatch finishLatch,
                                                                                   Proxy.Metadata metadata) {
        return (ClientPullResponseStreamObserver) localBeanFactory
                .getBean(ClientPullResponseStreamObserver.class, pipe, finishLatch, metadata);
    }

    public ClientPushResponseStreamObserver createClientPushResponseStreamObserver(ResultCallback<Proxy.Metadata> resultCallback,
                                                                                   CountDownLatch finishLatch) {
        return (ClientPushResponseStreamObserver) localBeanFactory
                .getBean(ClientPushResponseStreamObserver.class, resultCallback, finishLatch);
    }

    public ServerPushRequestStreamObserver
    createServerPushRequestStreamObserver(Pipe pipe,
                                          StreamObserver<Proxy.Metadata> responseObserver) {
        return (ServerPushRequestStreamObserver) localBeanFactory
                .getBean(ServerPushRequestStreamObserver.class, pipe, responseObserver);
    }

    public ClientUnaryCallResponseStreamObserver createClientUnaryCallResponseStreamObserver(Pipe pipe,
                                                                                             CountDownLatch finishLatch,
                                                                                             Proxy.Metadata metadata) {
        return (ClientUnaryCallResponseStreamObserver) localBeanFactory
                .getBean(ClientUnaryCallResponseStreamObserver.class, pipe, finishLatch, metadata);
    }
}
