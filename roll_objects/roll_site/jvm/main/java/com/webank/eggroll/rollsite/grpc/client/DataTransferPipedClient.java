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

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet;
import com.webank.eggroll.core.concurrent.AwaitSettableFuture;
import com.webank.eggroll.core.grpc.client.GrpcClientContext;
import com.webank.eggroll.core.grpc.client.GrpcClientTemplate;
import com.webank.eggroll.core.grpc.observer.SameTypeFutureCallerResponseStreamObserver;
import com.webank.eggroll.core.testgrpc.HelloCallerResponseStreamObserver;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.grpc.test.GrpcTest.HelloResponse;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStreamObserverFactory;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStubFactory;
import com.webank.eggroll.rollsite.grpc.observer.PushClientResponseStreamObserver;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.service.FdnRouter;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.webank.eggroll.core.util.ErrorUtils;

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

    /*
    public void push(Proxy.Metadata metadata, Pipe pipe) {
        String onelineStringMetadata = toStringUtils.toOneLineString(metadata);
        LOGGER.info("[PUSH][CLIENT] client send push to server: {}",
            onelineStringMetadata);
        DataTransferServiceGrpc.DataTransferServiceStub stub = getStub(metadata.getSrc(), metadata.getDst());

        try {
            Proxy.Topic from = metadata.getSrc();
            Proxy.Topic to = metadata.getDst();
            stub = getStub(from, to);
        } catch (Exception e) {
            LOGGER.error("[PUSH][CLIENT] error when creating push stub");
            pipe.onError(e);
        }

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final ResultCallback<Metadata> resultCallback = new SingleResultCallback<Metadata>();

        StreamObserver<Proxy.Metadata> responseObserver =
            proxyGrpcStreamObserverFactory.createClientPushResponseStreamObserver(resultCallback, finishLatch);

        StreamObserver<Proxy.Packet> requestObserver = stub.push(responseObserver);
        LOGGER.info("[PUSH][CLIENT] push stub: {}, metadata: {}",
            stub.getChannel(), onelineStringMetadata);

        int emptyRetryCount = 0;
        Proxy.Packet packet = null;
        do {
            //packet = (Proxy.Packet) pipe.read(1, TimeUnit.SECONDS);
            packet = (Proxy.Packet) pipe.read();

            if (packet != null) {
                requestObserver.onNext(packet);
                emptyRetryCount = 0;
            } else {
                ++emptyRetryCount;
                if (emptyRetryCount % 60 == 0) {
                    LOGGER.info("[PUSH][CLIENT] push stub waiting. empty retry count: {}, metadata: {}",
                        emptyRetryCount, onelineStringMetadata);
                }
            }
        } while ((packet != null || !pipe.isDrained()) && emptyRetryCount < 30 && !pipe.hasError());

        LOGGER.info("[PUSH][CLIENT] break out from loop. Proxy.Packet is null? {} ; pipe.isDrained()? {}" +
                ", pipe.hasError? {}, metadata: {}",
            packet == null, pipe.isDrained(), pipe.hasError(), onelineStringMetadata);

        if (pipe.hasError()) {
            Throwable error = pipe.getError();
            LOGGER.error("[PUSH][CLIENT] push error: {}, metadata: {}",
                ExceptionUtils.getStackTrace(error), onelineStringMetadata);
            requestObserver.onError(error);

            return;
        }

        requestObserver.onCompleted();
        try {
            finishLatch.await(MAX_AWAIT_HOURS, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error("[PUSH][CLIENT] client push: finishLatch.await() interrupted");
            requestObserver.onError(errorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
            Thread.currentThread().interrupt();
            return;
        }

        if (pipe instanceof PacketQueueSingleResultPipe) {
            PacketQueueSingleResultPipe convertedPipe = (PacketQueueSingleResultPipe) pipe;
            if (resultCallback.hasResult()) {
                convertedPipe.setResult(resultCallback.getResult());
            } else {
                LOGGER.warn("No Proxy.Metadata returned in pipe. request metadata: {}",
                    onelineStringMetadata);
            }
        }
        pipe.onComplete();

        LOGGER.info("[PUSH][CLIENT] push closing pipe. metadata: {}",
            onelineStringMetadata);
    }
    */

    //public synchronized void initPush(TransferBroker request, BasicMeta.Endpoint endpoint) {
    public synchronized void initPush(Proxy.Metadata metadata, Pipe pipe) {
        //LOGGER.info("[DEBUG][CLUSTERCOMM] initPush. broker: {}, transferMetaId: {}", pipe, toStringUtils.toOneLineString(request.getTransferMeta()));
        /*
        GrpcAsyncClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> asyncClientContext
            = transferServiceFactory.createPushClientGrpcAsyncClientContext();

        //BasicMeta.Endpoint.Builder builder = BasicMeta.Endpoint.newBuilder();
        endpoint = proxyGrpcStubFactory.getAsyncEndpoint(metadata.getDst());
        //endpoint = builder.setIp("192.168.1.101").setPort(9395).build();

        asyncClientContext.setLatchInitCount(1)
            .setEndpoint(endpoint)
            .setSecureRequest(defaultServerConf.isSecureClient())
            .setFinishTimeout(RuntimeConstants.DEFAULT_WAIT_TIME, RuntimeConstants.DEFAULT_TIMEUNIT)
            .setCallerStreamingMethodInvoker(DataTransferServiceGrpc.DataTransferServiceStub::push)
            .setCallerStreamObserverClassAndArguments(PushClientResponseStreamObserver.class, pipe)
            .setRequestStreamProcessorClassAndArguments(PushStreamProcessor.class, pipe);

        pushTemplate = transferServiceFactory.createPushClientTemplate();
        pushTemplate.setGrpcAsyncClientContext(asyncClientContext);

        pushTemplate.initCallerStreamingRpc();

        inited.compareAndSet(false, true);
        */
        GrpcClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Metadata> context
            = new GrpcClientContext<>();

        endpoint = proxyGrpcStubFactory.getAsyncEndpoint(metadata.getDst());

        AwaitSettableFuture<HelloResponse> delayedResult = new AwaitSettableFuture<>();
        context.setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class);

        //.setCallerStreamObserverClassAndInitArgs(SameTypeCallerResponseStreamObserver.class)
        LOGGER.info("ip: {}, Port: {}", endpoint.getIp(), endpoint.getPort());
        context.setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class)
            .setServerEndpoint(endpoint.getIp(), endpoint.getPort())
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
            LOGGER.info("[DEBUG][CLUSTERCOMM] proxyClient not inited yet");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("error in doPush: " + ExceptionUtils.getStackTrace(e));
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("[DEBUG][CLUSTERCOMM] doPush call processCallerStreamingRpc");
        pushTemplate.processCallerStreamingRpc();

    }

    public synchronized void completePush() {
        // LOGGER.info("[PUSH][CLIENT] completing push");
        if (pushTemplate == null) {
            throw new IllegalStateException("pushTemplate has not been initialized yet");
        }
        pushTemplate.completeStreamingRpc();
    }

    public void pull(Proxy.Metadata metadata, Pipe pipe) {
        String onelineStringMetadata = ToStringUtils.toOneLineString(metadata);
        LOGGER.info("[PULL][CLIENT] client send pull to server: {}", onelineStringMetadata);
        DataTransferServiceGrpc.DataTransferServiceStub stub = getStub(metadata.getDst(), metadata.getSrc());

        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<Proxy.Packet> responseObserver =
                proxyGrpcStreamObserverFactory.createClientPullResponseStreamObserver(pipe, finishLatch, metadata);

        stub.pull(metadata, responseObserver);
        LOGGER.info("[PULL][CLIENT] pull stub: {}, metadata: {}",
                stub.getChannel(), onelineStringMetadata);

        try {
            finishLatch.await(MAX_AWAIT_HOURS, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOGGER.error("[PULL][CLIENT] client pull: finishLatch.await() interrupted");
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

    public Proxy.Packet unaryCall(Proxy.Packet request, Pipe pipe) {
        GrpcClientContext<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet> context
            = new GrpcClientContext<>();

        Proxy.Metadata metadata = request.getHeader();
        //endpoint = proxyGrpcStubFactory.getAsyncEndpoint(metadata.getDst());
        System.out.println("ip:" + endpoint.getIp() + "port:" + endpoint.getPort());

        AwaitSettableFuture<Packet> delayedResult = new AwaitSettableFuture<>();
        context.setStubClass(DataTransferServiceGrpc.DataTransferServiceStub.class)
            .setServerEndpoint(endpoint.getIp(), endpoint.getPort())
            .setCalleeStreamingMethodInvoker(DataTransferServiceGrpc.DataTransferServiceStub::unaryCall)
            .setCallerStreamObserverClassAndInitArgs(SameTypeFutureCallerResponseStreamObserver.class,
                delayedResult);

        GrpcClientTemplate<DataTransferServiceGrpc.DataTransferServiceStub, Proxy.Packet, Proxy.Packet> template
            = new GrpcClientTemplate<>();
        template.setGrpcClientContext(context);

        //Proxy.Packet result = HelloRequest.newBuilder().setMsg("test hello | ").build();

        Proxy.Packet result = null;

        try {
            result = template.calleeStreamingRpcWithImmediateDelayedResult(request, delayedResult);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return result;

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

        LOGGER.info("[ROUTE] route info: {} routed to {}", ToStringUtils.toOneLineString(to),
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
