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

package com.webank.eggroll.rollsite.factory;

import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc;
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc.DataTransferServiceStub;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata;
import com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet;
import com.webank.eggroll.rollsite.grpc.core.api.grpc.client.GrpcAsyncClientContext;
import com.webank.eggroll.rollsite.grpc.core.api.grpc.client.GrpcStreamingClientTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class TransferServiceFactory {
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private GrpcStreamingClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> nonSpringPushTemplate;
    @Autowired
    private GrpcAsyncClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> nonSpringPushContext;
    @Autowired
    private GrpcStreamingClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet> nonSpringUnaryCallTemplate;
    @Autowired
    private GrpcAsyncClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet> nonSpringUnaryCallContext;
    public TransferServiceFactory() {
    }

    public GrpcAsyncClientContext<DataTransferServiceStub, Packet, Metadata>
    createPushClientGrpcAsyncClientContext() {
        return applicationContext.getBean(nonSpringPushContext.getClass())
                .setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class);
    }

    public GrpcAsyncClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet>
    createUnaryCallClientGrpcAsyncClientContext() {
        return applicationContext.getBean(nonSpringUnaryCallContext.getClass())
            .setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class);
    }

    public GrpcStreamingClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet>
    createUnaryCallClientTemplate() {
        return applicationContext.getBean(nonSpringUnaryCallTemplate.getClass());
    }

    public GrpcStreamingClientTemplate<DataTransferServiceStub, Packet, Metadata>
    createPushClientTemplate() {
        return applicationContext.getBean(nonSpringPushTemplate.getClass());
    }

}
