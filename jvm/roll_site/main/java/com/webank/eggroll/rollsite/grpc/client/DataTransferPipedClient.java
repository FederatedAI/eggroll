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

package com.webank.eggroll.rollsite.grpc.client;

import com.google.common.base.Preconditions;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.grpc.client.GrpcClientContext;
import com.webank.eggroll.core.grpc.client.GrpcClientTemplate;
import com.webank.eggroll.core.util.ErrorUtils;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStreamObserverFactory;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStubFactory;
import com.webank.eggroll.rollsite.grpc.observer.PushClientResponseStreamObserver;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.service.FdnRouter;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class DataTransferPipedClient {
    private static final Logger LOGGER = LogManager.getLogger(DataTransferPipedClient.class);
    @Autowired
    private ProxyGrpcStubFactory proxyGrpcStubFactory;
    @Autowired
    private ProxyGrpcStreamObserverFactory proxyGrpcStreamObserverFactory;
    @Autowired
    private FdnRouter fdnRouter;
    @Autowired
    private ProxyServerConf proxyServerConf;
    private BasicMeta.Endpoint endpoint;
    private boolean needSecureChannel;
    private long MAX_AWAIT_HOURS = 24;
    private AtomicBoolean inited = new AtomicBoolean(false);

    private Pipe pipe;

    //private GrpcStreamingClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> pushTemplate;
    GrpcClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> pushTemplate;

    public DataTransferPipedClient() {
        needSecureChannel = false;
    }

    //public synchronized void initPush(TransferBroker request, BasicMeta.Endpoint endpoint) {
    public synchronized void initPush(Proxy.Metadata metadata, Pipe pipe) {
        //LOGGER.info("[DEBUG][CLUSTERCOMM] initPush. broker: {}, transferMetaId: {}", pipe, toStringUtils.toOneLineString(request.getTransferMeta()));
        GrpcClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> context
            = new GrpcClientContext<>();

        endpoint = proxyGrpcStubFactory.getAsyncEndpoint(metadata.getDst());
        this.pipe = pipe;
        context.setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class);

        needSecureChannel = proxyServerConf.isSecureServer();

        //.setCallerStreamObserverClassAndInitArgs(SameTypeCallerResponseStreamObserver.class)
        LOGGER.trace("ip={}, Port={}", endpoint.getIp(), endpoint.getPort());
        context.setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class)
            .setServerEndpoint(endpoint.getIp(), endpoint.getPort())
            .setSecureRequest(needSecureChannel)
            .setCallerStreamingMethodInvoker(DataTransferServiceGrpc.DataTransferServiceStub::push)
            .setCallerStreamObserverClassAndInitArgs(PushClientResponseStreamObserver.class, pipe)
            .setRequestStreamProcessorClassAndArgs(PushStreamProcessor.class, pipe);

        pushTemplate = new GrpcClientTemplate<>();

        pushTemplate.setGrpcClientContext(context);
        pushTemplate.initCallerStreamingRpc();

        inited.compareAndSet(false, true);
        /*
        for (int i = 0; i < 2; ++i) {
            template.processCallerStreamingRpc();
        }

        template.completeStreamingRpc();
        */

    }

    public void doPush() {

        if (pushTemplate == null) {
            throw new IllegalStateException("pushTemplate has not been initialized yet");
        }

        while (!inited.get()) {
            LOGGER.trace("[DEBUG][ROLLSITE] proxyClient not inited yet");

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOGGER.error("error in doPush", e);
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.trace("[DEBUG][ROLLSITE] doPush call processCallerStreamingRpc");

        if (pipe.hasError()) {
            pushTemplate.errorCallerStreamingRpc(pipe.getError());
        } else {
            pushTemplate.processCallerStreamingRpc();
        }
    }

    public synchronized void completePush() {
        // LOGGER.info("[PUSH][CLIENT] completing push");
        if (pushTemplate == null) {
            throw new IllegalStateException("pushTemplate has not been initialized yet");
        }
        if (!pipe.hasError()) {
            pushTemplate.completeStreamingRpc();
        }
    }

    public void pull(Proxy.Metadata metadata, Pipe pipe) {
        String onelineStringMetadata = ToStringUtils.toOneLineString(metadata);
        LOGGER.debug("[PULL][CLIENT] client send pull to server={}", onelineStringMetadata);
        DataTransferServiceGrpc.DataTransferServiceStub stub = getStub(metadata.getDst(), metadata.getSrc());

        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<Proxy.Packet> responseObserver =
                proxyGrpcStreamObserverFactory.createClientPullResponseStreamObserver(pipe, finishLatch, metadata);

        stub.pull(metadata, responseObserver);
        LOGGER.trace("[PULL][CLIENT] pull stub={}, metadata={}",
                stub.getChannel(), onelineStringMetadata);

        try {
            finishLatch.await(MAX_AWAIT_HOURS, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error("[PULL][CLIENT] client pull: finishLatch.await() interrupted. metadata={}", onelineStringMetadata);
            responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
            Thread.currentThread().interrupt();
            return;
        }

        responseObserver.onCompleted();
    }

    /*
    public void unaryCall_(Proxy.Packet packet, Pipe pipe) {
        Preconditions.checkNotNull(packet);
        Proxy.Metadata header = packet.getHeader();
        String onelineStringMetadata = ToStringUtils.toOneLineString(header);
        LOGGER.info("[UNARYCALL][CLIENT] client send unary call to server: {}", onelineStringMetadata);
        //LOGGER.info("[UNARYCALL][CLIENT] packet: {}", ToStringUtils.toOneLineString(packet));

        DataTransferServiceGrpc.DataTransferServiceStub stub = getStub(
                packet.getHeader().getSrc(), packet.getHeader().getDst());

        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<Proxy.Packet> responseObserver = proxyGrpcStreamObserverFactory
                .createClientUnaryCallResponseStreamObserver(pipe, finishLatch, packet.getHeader());
        stub.unaryCall(packet, responseObserver);

        LOGGER.info("[UNARYCALL][CLIENT] unary call stub: {}, metadata: {}",
                stub.getChannel(), onelineStringMetadata);

        try {
            finishLatch.await(MAX_AWAIT_HOURS, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error("[UNARYCALL][CLIENT] client unary call: finishLatch.await() interrupted");
            responseObserver.onError(errorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
            Thread.currentThread().interrupt();
            return;
        }

        responseObserver.onCompleted();
    }
    */


    public void unaryCall(Proxy.Packet packet, Pipe pipe) {
        Preconditions.checkNotNull(packet);
        Proxy.Metadata header = packet.getHeader();
        String oneLineStringMetadata = ToStringUtils.toOneLineString(header);
        LOGGER.debug("[UNARYCALL][CLIENT] client send unary call to server. metadata={}", oneLineStringMetadata);

        DataTransferServiceGrpc.DataTransferServiceStub stub = getStub(
                packet.getHeader().getSrc(), packet.getHeader().getDst());

        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<Proxy.Packet> responseObserver = proxyGrpcStreamObserverFactory
                .createClientUnaryCallResponseStreamObserver(pipe, finishLatch, packet.getHeader());
        stub.unaryCall(packet, responseObserver);

        try {
            finishLatch.await(MAX_AWAIT_HOURS, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error("[UNARYCALL][CLIENT] client unary call: finishLatch.await() interrupted");
            responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
            Thread.currentThread().interrupt();
            return;
        }

        responseObserver.onCompleted();
    }

    public void unaryCall2(Proxy.Packet request, Pipe pipe, boolean initialize) {
        Proxy.Metadata header = request.getHeader();
        String oneLineStringMetadata = ToStringUtils.toOneLineString(header);
        LOGGER.debug("[UNARYCALL][CLIENT] client send unary call to server. metadata={}", oneLineStringMetadata);
        GrpcClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet> context
            = new GrpcClientContext<>();

        Proxy.Metadata metadata = request.getHeader();

        DataTransferServiceGrpc.DataTransferServiceStub stub = getStub(
                request.getHeader().getSrc(), request.getHeader().getDst());

        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<Proxy.Packet> responseObserver = proxyGrpcStreamObserverFactory
                .createClientUnaryCallResponseStreamObserver(pipe, finishLatch, request.getHeader());
        stub.unaryCall(request, responseObserver);

        LOGGER.info("[UNARYCALL][CLIENT] unary call stub={}, metadata={}",
                stub.getChannel(), oneLineStringMetadata);

        try {
            finishLatch.await(MAX_AWAIT_HOURS, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error("[UNARYCALL][CLIENT] client unary call: finishLatch.await() interrupted");
            responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
            Thread.currentThread().interrupt();
            return;
        }

        responseObserver.onCompleted();
//        AwaitSettableFuture<Packet> delayedResult = new AwaitSettableFuture<>();
//        context.setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class)
//            .setServerEndpoint(endpoint.getIp(), endpoint.getPort())
//            .setCalleeStreamingMethodInvoker(DataTransferServiceGrpc.DataTransferServiceStub::unaryCall)
//            .setCallerStreamObserverClassAndInitArgs(SameTypeFutureCallerResponseStreamObserver.class,
//                delayedResult);
//
//        GrpcClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet> template
//            = new GrpcClientTemplate<>();
//        template.setGrpcClientContext(context);
//
//        Proxy.Packet result = null;
//        try {
//            result = template.calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult);
//        } catch (ExecutionException | InterruptedException e) {
//            LOGGER.error("error getting result", e);
//            throw new RuntimeException(e);
//        }

//        return result;

        /*
        AwaitSettableFuture<HelloResponse> delayedResult = new AwaitSettableFuture<>();
        context.setStubClass(HelloServiceStub.class)
            .setServerEndpoint("localhost", 50000)
            .setCalleeStreamingMethodInvoker(HelloServiceStub::unaryCall)
            .setCallerStreamObserverClassAndInitArgs(SameTypeFutureCallerResponseStreamObserver.class,
                delayedResult);

        GrpcClientTemplate<HelloServiceStub, HelloRequest, HelloResponse> template
            = new GrpcClientTemplate<>();
        template.setGrpcClientContext(context);

        HelloRequest request = HelloRequest.newBuilder().setMsg("test hello | ").build();

        HelloResponse result = template
            .calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult);

        return result.getMsg();
        */
    }


    private DataTransferServiceGrpc.DataTransferServiceStub getStub(Proxy.Topic from, Proxy.Topic to) {
        if (endpoint == null && !fdnRouter.isAllowed(from, to)) {
            throw new SecurityException("no permission from " + ToStringUtils.toOneLineString(from)
                    + " to " + ToStringUtils.toOneLineString(to));
        }

        DataTransferServiceGrpc.DataTransferServiceStub stub = null;
        if (endpoint == null) {
            stub = proxyGrpcStubFactory.getAsyncStub(to);
        } else {
            stub = proxyGrpcStubFactory.getAsyncStub(endpoint);
        }

        LOGGER.debug("[ROUTE] route info={} routed to endpoint={}", ToStringUtils.toOneLineString(to),
                ToStringUtils.toOneLineString(fdnRouter.route(to)));

        fdnRouter.route(from);

        return stub;
    }

    public boolean isNeedSecureChannel() {
        return needSecureChannel;
    }

    public void setNeedSecureChannel(boolean needSecureChannel) {
        this.needSecureChannel = needSecureChannel;
    }

    public BasicMeta.Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(BasicMeta.Endpoint endpoint) {
        this.endpoint = endpoint;
    }
}
