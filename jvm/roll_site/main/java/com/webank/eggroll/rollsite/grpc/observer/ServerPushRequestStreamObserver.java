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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.ai.eggroll.api.core.BasicMeta.Endpoint;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.meta.ErRollSiteHeader;
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes;
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader;
import com.webank.eggroll.core.util.ErrorUtils;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.RollSiteUtil;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.factory.EventFactory;
import com.webank.eggroll.rollsite.factory.PipeFactory;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStubFactory;
import com.webank.eggroll.rollsite.helper.ModelValidationHelper;
import com.webank.eggroll.rollsite.infra.JobStatus;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueuePipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueueSingleResultPipe;
import com.webank.eggroll.rollsite.manager.StatsManager;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.model.StreamStat;
import com.webank.eggroll.rollsite.utils.Timeouts;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import scala.collection.immutable.Map.Map1;


@Component
@Scope("prototype")
public class ServerPushRequestStreamObserver implements StreamObserver<Proxy.Packet> {
    private static final Logger LOGGER = LogManager.getLogger(ServerPushRequestStreamObserver.class);
    private static final Logger AUDIT = LogManager.getLogger("audit");
    private static final Logger DEBUGGING = LogManager.getLogger("debugging");
    private final StreamObserver<Proxy.Metadata> responseObserver;
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    @Autowired
    private EventFactory eventFactory;
    @Autowired
    private ModelValidationHelper modelValidationHelper;
    @Autowired
    private Timeouts timeouts;
    @Autowired
    private StatsManager statsManager;
    @Autowired
    private ProxyServerConf proxyServerConf;
    @Autowired
    private ProxyGrpcStubFactory proxyGrpcStubFactory;
    private Pipe pipe;
    private Proxy.Metadata inputMetadata;
    private StreamStat streamStat;
    private String myCoordinator;
    private long overallStartTimestamp;
    private long overallTimeout;
    private long completionWaitTimeout;
    private String oneLineStringInputMetadata;
    private boolean noError;
    private boolean isAuditEnabled;
    private boolean isDebugEnabled;
    private AtomicLong ackCount;
    private volatile boolean inited = false;
    private Proxy.Metadata response;
    private RollSiteUtil rollSiteUtil;

    public ServerPushRequestStreamObserver(PipeFactory pipeFactory, StreamObserver<Proxy.Metadata> responseObserver) {
        this.responseObserver = responseObserver;
        this.completionWaitTimeout = Timeouts.DEFAULT_COMPLETION_WAIT_TIMEOUT;
        this.overallTimeout = Timeouts.DEFAULT_OVERALL_TIMEOUT;

        this.noError = true;
        this.ackCount = new AtomicLong(0L);
    }

    public synchronized void init(Proxy.Metadata metadata) {
        if (inited) {
            return;
        }

        LOGGER.debug("[PUSH][SERVER] init metadata={}",
            ToStringUtils.toOneLineString(metadata));
        this.response = metadata;
        this.inputMetadata = metadata;
        this.inited = true;
    }

    @Override
    public void onNext(Proxy.Packet packet) {
        try {
            if (!inited) {
                init(packet.getHeader());
            }

            inputMetadata = packet.getHeader();

            streamStat = new StreamStat(inputMetadata, StreamStat.PUSH);
            oneLineStringInputMetadata = ToStringUtils.toOneLineString(inputMetadata);
            statsManager.add(streamStat);

            LOGGER.trace("[PUSH][SERVER][ONNEXT] metadata={}, data size={}",
                oneLineStringInputMetadata, packet.getBody().getValue().size());

            if (StringUtils.isBlank(myCoordinator)) {
                myCoordinator = proxyServerConf.getCoordinator();
            }

            if (inputMetadata.hasConf()) {
                overallTimeout = timeouts.getOverallTimeout(inputMetadata);
                completionWaitTimeout = timeouts.getCompletionWaitTimeout(inputMetadata);
            }

            isAuditEnabled = proxyServerConf.isAuditEnabled();
            isDebugEnabled = proxyServerConf.isDebugEnabled();

            // check if topics are valid
            if (!modelValidationHelper.checkTopic(inputMetadata.getDst())
                || !modelValidationHelper.checkTopic(inputMetadata.getSrc())) {
                onError(new IllegalArgumentException(
                    "At least one of topic name, coordinator, role is blank."));
                noError = false;
                return;
            }

            pipe = new PacketQueueSingleResultPipe(inputMetadata);
            if (noError) {
                pipe.write(packet);
                ackCount.incrementAndGet();
                if (isAuditEnabled && packet.getHeader().getSrc().getPartyId()
                    .equals(myCoordinator)) {
                    AUDIT.info(ToStringUtils.toOneLineString(packet));
                }

                overallStartTimestamp = System.currentTimeMillis();

                if (!proxyServerConf.getPartyId().equals(inputMetadata.getDst().getPartyId())) {
                    PipeHandleNotificationEvent event =
                        eventFactory.createPipeHandleNotificationEvent(
                            this, PipeHandleNotificationEvent.Type.PUSH, inputMetadata, pipe);
                    applicationEventPublisher.publishEvent(event);
                } else {
                    ByteString value = packet.getBody().getValue();
                    String name = packet.getHeader().getTask().getModel().getName();
                    String namespace = packet.getHeader().getTask().getModel().getDataKey();
                    LOGGER.trace("[SEND][SERVER][OBSERVER] ready to putBatch for namespace={}, name={}",
                        namespace, name);

                    // TODO:0: better wait
                    if (rollSiteUtil == null) {
                        // TODO:0: change this when delim changes
                        ErRollSiteHeader rollSiteHeader = null;
                        try {
                            rollSiteHeader = TransferModelPbMessageSerdes
                                .ErRollSiteHeaderFromPbMessage(
                                    RollSiteHeader
                                        .parseFrom(name.getBytes(StandardCharsets.ISO_8859_1)))
                                .fromProto();
                        } catch (InvalidProtocolBufferException e) {
                            LOGGER.error("error parsing roll site header", e);
                            onError(e);
                        }
                        String job_id = rollSiteHeader.rollSiteSessionId();
                        try {
                            while (!JobStatus.isJobIdToSessionRegistered(job_id)) {
                                Thread.sleep(20);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        String sessionId = JobStatus.getErSessionId(job_id);
                        if (sessionId != null) {
                            rollSiteUtil = new RollSiteUtil(sessionId, rollSiteHeader,
                                new Map1<>("job_id_tag", Thread.currentThread().getName()));
                        } else {
                            Throwable t = new IllegalArgumentException("session id does not exist");
                            onError(t);
                        }
                    }

                    if (value == null) {
                        IllegalStateException e = new IllegalStateException(
                            "value is null for name: " + name);
                        onError(e);
                        throw e;
                    }

                    rollSiteUtil.putBatch(value);
                    // for putBatch, on complete here; for cascaded call, on complete at cascaded call
                    pipe.onComplete();
                    LOGGER.trace("[SEND][SERVER][OBSERVER] end putBatch for namespace={}, name={}", namespace, name);
                }

                if (timeouts.isTimeout(overallTimeout, overallStartTimestamp)) {
                    Throwable error = new IllegalStateException(
                        "push overall wait timeout exceeds overall timeout: " + overallTimeout
                            + ", metadata: " + oneLineStringInputMetadata);
                    pipe.onError(error);
                    onError(error);
                    return;
                }

                if (isDebugEnabled) {
                    DEBUGGING.info("[PUSH][SERVER][ONNEXT] server: {}, ackCount: {}", packet,
                        ackCount.get());
                    if (packet.getBody() != null && packet.getBody().getValue() != null) {
                        ByteString value = packet.getBody().getValue();
                        streamStat.increment(value.size());
                        DEBUGGING.info("[PUSH][SERVER][ONNEXT] length: {}, metadata: {}",
                            packet.getBody().getValue().size(), oneLineStringInputMetadata);
                    } else {
                        DEBUGGING.info("[PUSH][SERVER][ONNEXT] length : null, metadata: {}",
                            oneLineStringInputMetadata);
                    }
                    DEBUGGING.info("-------------");
                }
            }
        } catch (Exception e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        Endpoint next = proxyGrpcStubFactory.getAsyncEndpoint(inputMetadata.getDst());
        StringBuilder builder = new StringBuilder();
        builder.append("src: ")
            .append(ToStringUtils.toOneLineString(inputMetadata.getSrc()))
            .append(", dst: ")
            .append(ToStringUtils.toOneLineString(inputMetadata.getDst()))
            .append(", my hop: ")
            .append(proxyServerConf.getPartyId())
            .append(":")
            .append(proxyServerConf.getPort())
            .append(" -> next hop: ")
            .append(next.getIp())
            .append(":")
            .append(next.getPort());
        RuntimeException exceptionWithHop = new RuntimeException(builder.toString(), throwable);
        LOGGER.error("[PUSH][OBSERVER][ONERROR] error in push server. metadata={}, ackCount={}",
                oneLineStringInputMetadata, ackCount.get(), exceptionWithHop);
        LOGGER.error(ExceptionUtils.getStackTrace(exceptionWithHop));

        pipe.setDrained();

        pipe.onError(exceptionWithHop);
        responseObserver.onError(ErrorUtils.toGrpcRuntimeException(exceptionWithHop));
        streamStat.onError();
    }

    @Override
    public void onCompleted() {
        long lastestAckCount = ackCount.get();
        LOGGER.trace("[PUSH][SERVER][ONCOMPLETE] onCompleted for metadata={}. ackCount={}", oneLineStringInputMetadata, lastestAckCount);

        long completionWaitStartTimestamp = System.currentTimeMillis();
        long loopEndTimestamp = completionWaitStartTimestamp;
        long waitCount = 0;

        if (inputMetadata == null) {
            IllegalStateException e = new IllegalStateException("input metadata is null in onComplete");
            onError(e);
            throw e;
        }

        pipe.setDrained();

        while (!pipe.isClosed()
                && !timeouts.isTimeout(completionWaitTimeout, completionWaitStartTimestamp, loopEndTimestamp)
                && !timeouts.isTimeout(overallTimeout, overallStartTimestamp, loopEndTimestamp)) {
            try {
                pipe.awaitClosed(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                loopEndTimestamp = System.currentTimeMillis();
            }
            if (++waitCount % 60 == 0) {
                String extraInfo = "";
                if (pipe instanceof PacketQueuePipe) {
                    PacketQueuePipe pqp = (PacketQueuePipe) pipe;
                    extraInfo = "queueSize: " + pqp.getQueueSize();
                }
                LOGGER.trace("[PUSH][SERVER][ONCOMPLETE] waiting push to complete. wait time={}. metadata={}, extrainfo={}",
                        (loopEndTimestamp - completionWaitStartTimestamp), oneLineStringInputMetadata, extraInfo);
            }
        }

        try {
            if (pipe.hasError()) {
                onError(pipe.getError());
            } else if (timeouts.isTimeout(completionWaitTimeout, completionWaitStartTimestamp, loopEndTimestamp)) {
                StringBuilder errmsgBuilder = new StringBuilder();
                errmsgBuilder.append("[PUSH][SERVER][ONCOMPLETE] push server completion wait exceeds completionWaitTimeout. ")
                    .append("completionWaitTimeout=")
                    .append(completionWaitTimeout)
                    .append(", metadata=")
                    .append(oneLineStringInputMetadata)
                    .append(", completionWaitStartTimestamp=")
                    .append(completionWaitStartTimestamp)
                    .append(", loopEndTimestamp=")
                    .append(loopEndTimestamp)
                    .append(", ackCount=")
                    .append(lastestAckCount);

                String errmsg = errmsgBuilder.toString();
                LOGGER.error(errmsg);
                responseObserver.onError(new TimeoutException(errmsg));
                streamStat.onError();
            } else if (timeouts.isTimeout(overallTimeout, overallStartTimestamp, loopEndTimestamp)) {
                StringBuilder errmsgBuilder = new StringBuilder();
                errmsgBuilder.append("[PUSH][SERVER][ONCOMPLETE] push server overall time exceeds overallTimeout ")
                    .append("overallTimeout=")
                    .append(overallTimeout)
                    .append(", metadata=")
                    .append(oneLineStringInputMetadata)
                    .append(", completionWaitStartTimestamp=")
                    .append(completionWaitStartTimestamp)
                    .append(", loopEndTimestamp=")
                    .append(loopEndTimestamp)
                    .append(", ackCount=")
                    .append(lastestAckCount);

                String errmsg = errmsgBuilder.toString();
                LOGGER.error(errmsg);
                responseObserver.onError(new TimeoutException(errmsg));
                streamStat.onError();
            } else {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                LOGGER.debug("[PUSH][SERVER][ONCOMPLETE] push server complete. inputMetadata={}",
                        ToStringUtils.toOneLineString(response));
                streamStat.onComplete();
            }
        } catch (NullPointerException e) {
            LOGGER.error("[PUSH][SERVER][ONCOMPLETE] NullPointerException caught in push onComplete. metadata={}",
                    oneLineStringInputMetadata);
        }
    }
}
