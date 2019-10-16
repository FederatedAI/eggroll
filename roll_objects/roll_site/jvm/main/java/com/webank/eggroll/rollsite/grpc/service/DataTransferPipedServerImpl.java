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

package com.webank.eggroll.rollsite.grpc.service;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.util.ErrorUtils;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.factory.EventFactory;
import com.webank.eggroll.rollsite.factory.PipeFactory;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStreamObserverFactory;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueueSingleResultPipe;
import com.webank.eggroll.rollsite.utils.Timeouts;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class DataTransferPipedServerImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {
    private static final Logger LOGGER = LogManager.getLogger(DataTransferPipedServerImpl.class);
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    @Autowired
    private ProxyGrpcStreamObserverFactory proxyGrpcStreamObserverFactory;
    @Autowired
    private Timeouts timeouts;
    @Autowired
    private EventFactory eventFactory;
    private Pipe defaultPipe;
    private PipeFactory pipeFactory;

    static Map<String, PacketQueueSingleResultPipe> pipeMap = Maps.newConcurrentMap();

    @Override
    public StreamObserver<Proxy.Packet> push(StreamObserver<Proxy.Metadata> responseObserver) {
        LOGGER.info("[PUSH][SERVER] request received");

        //Pipe pipe = getPipe("modelA");
        // LOGGER.info("push pipe: {}", pipe);
/*
        PipeHandleNotificationEvent event =
                eventFactory.createPipeHandleNotificationEvent(
                        this, PipeHandleNotificationEvent.Type.PUSH, null, pipe);
        applicationEventPublisher.publishEvent(event);
*/

        StreamObserver<Proxy.Packet> requestObserver;
        requestObserver = proxyGrpcStreamObserverFactory
                .createServerPushRequestStreamObserver(pipeFactory, responseObserver);

        return requestObserver;
    }

    @Override
    public void pull(Proxy.Metadata inputMetadata, StreamObserver<Proxy.Packet> responseObserver) {
        String oneLineStringInputMetadata = ToStringUtils.toOneLineString(inputMetadata);
        LOGGER.info("[PULL][SERVER] request received. metadata: {}",
                oneLineStringInputMetadata);

        long overallTimeout = timeouts.getOverallTimeout(inputMetadata);
        long packetIntervalTimeout = timeouts.getPacketIntervalTimeout(inputMetadata);

        //Pipe pipe = getPipe("model_A");
        Pipe pipe = getPipe(inputMetadata.getTask().getModel().getName());

        LOGGER.info("[PULL][SERVER] pull pipe: {}", pipe);

        /*
        PipeHandleNotificationEvent event =
                eventFactory.createPipeHandleNotificationEvent(
                        this, PipeHandleNotificationEvent.Type.PULL, inputMetadata, pipe);
        applicationEventPublisher.publishEvent(event);
        */

        long startTimestamp = System.currentTimeMillis();
        long lastPacketTimestamp = startTimestamp;
        long loopEndTimestamp = lastPacketTimestamp;

        Proxy.Packet packet = null;
        boolean hasReturnedBefore = false;
        int emptyRetryCount = 0;
        Proxy.Packet lastReturnedPacket = null;

        while ((!hasReturnedBefore || !pipe.isDrained())
                && !pipe.hasError()
                && !timeouts.isTimeout(packetIntervalTimeout, lastPacketTimestamp, loopEndTimestamp)
                && !timeouts.isTimeout(overallTimeout, startTimestamp, loopEndTimestamp)) {
            packet = (Proxy.Packet) pipe.read(1, TimeUnit.SECONDS);
            // LOGGER.info("packet is null: {}", Proxy.Packet == null);
            loopEndTimestamp = System.currentTimeMillis();
            if (packet != null) {
                Proxy.Metadata outputMetadata = packet.getHeader();
                Proxy.Data outData = packet.getBody();
                LOGGER.info("PushStreamProcessor processing metadata: {}", ToStringUtils.toOneLineString(outputMetadata));
                LOGGER.info("PushStreamProcessor processing outData: {}", ToStringUtils.toOneLineString(outData));

                // LOGGER.info("server pull onNext()");
                responseObserver.onNext(packet);
                hasReturnedBefore = true;
                lastReturnedPacket = packet;
                lastPacketTimestamp = loopEndTimestamp;
                emptyRetryCount = 0;
            } else {
                long currentPacketInterval = loopEndTimestamp - lastPacketTimestamp;
                if (++emptyRetryCount % 60 == 0) {
                    LOGGER.info("[PULL][SERVER] pull waiting. current packetInterval: {}, packetIntervalTimeout: {}, metadata: {}",
                            currentPacketInterval, packetIntervalTimeout, oneLineStringInputMetadata);
                }
                break;
            }
        }

        boolean hasError = true;
        if (pipe.hasError()) {
            Throwable error = pipe.getError();
            LOGGER.error("[PULL][SERVER] pull finish with error: {}", ExceptionUtils.getStackTrace(error));
            responseObserver.onError(error);

            return;
        }

        StringBuilder sb = new StringBuilder();
        if (timeouts.isTimeout(packetIntervalTimeout, lastPacketTimestamp, loopEndTimestamp)) {
            sb.append("[PULL][SERVER] pull server error: Proxy.Packet interval exceeds timeout: ")
                    .append(packetIntervalTimeout)
                    .append(", metadata: ")
                    .append(oneLineStringInputMetadata)
                    .append(", lastPacketTimestamp: ")
                    .append(lastPacketTimestamp)
                    .append(", loopEndTimestamp: ")
                    .append(loopEndTimestamp);

            String errorMsg = sb.toString();

            LOGGER.error(errorMsg);

            TimeoutException e = new TimeoutException(errorMsg);
            responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
        } else if (timeouts.isTimeout(overallTimeout, startTimestamp, loopEndTimestamp)) {
            sb.append("[PULL][SERVER] pull server error: overall process time exceeds timeout: ")
                    .append(overallTimeout)
                    .append(", metadata: ")
                    .append(oneLineStringInputMetadata)
                    .append(", startTimestamp: ")
                    .append(startTimestamp)
                    .append(", loopEndTimestamp: ")
                    .append(loopEndTimestamp);
            String errorMsg = sb.toString();
            LOGGER.error(errorMsg);

            TimeoutException e = new TimeoutException(errorMsg);
            responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
            pipe.onError(e);
        } else {
            responseObserver.onCompleted();
            hasError = false;
            pipe.onComplete();
        }
        LOGGER.info("[PULL][SERVER] server pull finshed. hasReturnedBefore: {}, hasError: {}, metadata: {}",
                hasReturnedBefore, hasError, oneLineStringInputMetadata);
        //LOGGER.warn("pull last returned packet: {}", lastReturnedPacket);
    }

    @Override
    public void unaryCall(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Packet packet = null;
        boolean hasReturnedBefore = false;
        int emptyRetryCount = 0;

        Proxy.Metadata inputMetadata = request.getHeader();
        String oneLineStringInputMetadata = ToStringUtils.toOneLineString(inputMetadata);
        LOGGER.info("[UNARYCALL][SERVER] server unary request received. src: {}, dst: {}",
                ToStringUtils.toOneLineString(inputMetadata.getSrc()),
                ToStringUtils.toOneLineString(inputMetadata.getDst()));

        long overallTimeout = timeouts.getOverallTimeout(inputMetadata);
        long packetIntervalTimeout = timeouts.getPacketIntervalTimeout(inputMetadata);

        //Pipe pipe = getPipe("model_A");
        Pipe pipe = getPipe(inputMetadata.getTask().getModel().getName());

        LOGGER.info("[UNARYCALL][SERVER] unary call pipe: {}", pipe);

        if(request.getHeader().getOperator().equals("registerBroker")) {
            //String routeTable = ;
            //updateRouteTable(routeTable);
            Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();
            Proxy.Data data = Proxy.Data.newBuilder().setValue(ByteString.copyFromUtf8("hello")).build();
            packet = packetBuilder.setHeader(request.getHeader())
                                  .setBody(data)
                                  .build();
            responseObserver.onNext(packet);
            responseObserver.onCompleted();
            return;
        }

        PipeHandleNotificationEvent event =
                eventFactory.createPipeHandleNotificationEvent(
                        this, PipeHandleNotificationEvent.Type.UNARY_CALL, request, pipe);
        applicationEventPublisher.publishEvent(event);

        long startTimestamp = System.currentTimeMillis();
        long lastPacketTimestamp = startTimestamp;
        long loopEndTimestamp = System.currentTimeMillis();
        while ((!hasReturnedBefore || !pipe.isDrained())
                && !pipe.hasError()
                && !timeouts.isTimeout(overallTimeout, startTimestamp, loopEndTimestamp)) {
            packet = (Proxy.Packet) pipe.read(1, TimeUnit.SECONDS);
            loopEndTimestamp = System.currentTimeMillis();
            if (packet != null) {
                // LOGGER.info("server pull onNext()");
                responseObserver.onNext(packet);
                hasReturnedBefore = true;
                emptyRetryCount = 0;
                break;
            } else {
                long currentOverallWaitTime = loopEndTimestamp - lastPacketTimestamp;

                if (++emptyRetryCount % 60 == 0) {
                    LOGGER.info("[UNARYCALL][SERVER] unary call waiting. current overallWaitTime: {}, packetIntervalTimeout: {}, metadata: {}",
                            currentOverallWaitTime, packetIntervalTimeout, oneLineStringInputMetadata);
                }
            }
        }
        boolean hasError = true;

        if (pipe.hasError()) {
            Throwable error = pipe.getError();
            LOGGER.error("[UNARYCALL][SERVER] unary call finish with error: {}", ExceptionUtils.getStackTrace(error));
            responseObserver.onError(error);

            return;
        }

        if (!hasReturnedBefore) {
            if (timeouts.isTimeout(overallTimeout, startTimestamp, loopEndTimestamp)) {
                String errorMsg = "[UNARYCALL][SERVER] unary call server error: overall process time exceeds timeout: " + overallTimeout
                        + ", metadata: " + oneLineStringInputMetadata
                        + ", lastPacketTimestamp: " + lastPacketTimestamp
                        + ", loopEndTimestamp: " + loopEndTimestamp;
                LOGGER.error(errorMsg);

                TimeoutException e = new TimeoutException(errorMsg);
                responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
                pipe.onError(e);
            } else {
                String errorMsg = "[PULL][SERVER] pull server error: overall process time exceeds timeout: " + overallTimeout
                        + ", metadata: " + oneLineStringInputMetadata
                        + ", startTimestamp: " + startTimestamp
                        + ", loopEndTimestamp: " + loopEndTimestamp;

                TimeoutException e = new TimeoutException(errorMsg);
                responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
                pipe.onError(e);
            }
        } else {
            hasError = false;
            responseObserver.onCompleted();
            pipe.onComplete();
        }

        LOGGER.info("[UNARYCALL][SERVER] server unary call completed. hasReturnedBefore: {}, hasError: {}, metadata: {}",
                hasReturnedBefore, hasError, oneLineStringInputMetadata);
    }

    private void checkNotNull() {
        if (defaultPipe == null && pipeFactory == null) {
            throw new NullPointerException("defaultPipe and pipeFactory are both null");
        }
    }

    private Pipe getPipe(String name) {
        checkNotNull();

        Pipe result = defaultPipe;
        if (pipeFactory != null) {
            result = pipeFactory.create(name);
        }

        return result;
    }

    public void setDefaultPipe(Pipe defaultPipe) {
        this.defaultPipe = defaultPipe;
    }

    public void setPipeFactory(PipeFactory pipeFactory) {
        this.pipeFactory = pipeFactory;
    }
}
