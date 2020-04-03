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
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.ErRollSiteHeader;
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes;
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader;
import com.webank.eggroll.core.util.ErrorUtils;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.RollSiteUtil;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.factory.EventFactory;
import com.webank.eggroll.rollsite.factory.PipeFactory;
import com.webank.eggroll.rollsite.helper.ModelValidationHelper;
import com.webank.eggroll.rollsite.infra.JobStatus;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueuePipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueueSingleResultPipe;
import com.webank.eggroll.rollsite.manager.StatsManager;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.model.StreamStat;
import com.webank.eggroll.rollsite.utils.PipeUtils;
import com.webank.eggroll.rollsite.utils.Timeouts;
import io.grpc.Grpc;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import scala.collection.immutable.Map.Map1;

/*
class putBatchThread extends Thread{
    private Proxy.Packet inputPacket;

    public putBatchThread(Proxy.Packet packet)
    {
        this.inputPacket = packet;
    }

    @Override
    public void run() {
        ByteString value = inputPacket.getBody().getValue();
        String name = inputPacket.getHeader().getTask().getModel().getName();
        String namespace = inputPacket.getHeader().getTask().getModel().getDataKey();
        RollSiteUtil.putBatch(name, namespace, value.asReadOnlyByteBuffer());
    }

}
*/

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
    private ApplicationContext applicationContext;
    @Autowired
    private ThreadPoolTaskExecutor asyncThreadPool;
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
    private PipeUtils pipeUtils;
    private Pipe pipe;
    private PipeFactory pipeFactory;
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
        //this.pipe = pipe;
        this.pipeFactory = pipeFactory;
        //this.pipeMap = pipeMap;
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

        this.response = metadata;
        this.inited = true;
    }

    @Override
    public void onNext(Proxy.Packet packet) {
        LOGGER.info("[SEND][SERVER][OBSERVER][ONNEXT] header: {}", ToStringUtils.toOneLineString(packet.getHeader()));
        if (!inited) {
            init(packet.getHeader());
        }

        inputMetadata = packet.getHeader();
        LOGGER.info("inputMetadata.getTask().getTaskId():{}", inputMetadata.getTask().getTaskId());

        streamStat = new StreamStat(inputMetadata, StreamStat.PUSH);
        oneLineStringInputMetadata = ToStringUtils.toOneLineString(inputMetadata);
        statsManager.add(streamStat);

        LOGGER.info(Grpc.TRANSPORT_ATTR_REMOTE_ADDR.toString());

        LOGGER.info("[PUSH][OBSERVER][ONNEXT] metadata: {}", oneLineStringInputMetadata);
        LOGGER.info("[PUSH][OBSERVER][ONNEXT] request src: {}, dst: {}, data size: {}",
                ToStringUtils.toOneLineString(inputMetadata.getSrc()),
                ToStringUtils.toOneLineString(inputMetadata.getDst()),
                packet.getBody().getValue().size());

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
            onError(new IllegalArgumentException("At least one of topic name, coordinator, role is blank."));
            noError = false;
            return;
        }

        LOGGER.info("model name: {}", inputMetadata.getTask().getModel().getName());

        pipe = new PacketQueueSingleResultPipe(inputMetadata);
        if (noError) {
            pipe.write(packet);
            ackCount.incrementAndGet();
            //LOGGER.info("myCoordinator: {}, Proxy.Packet coordinator: {}", myCoordinator, packet.getHeader().getSrc().getCoordinator());
            if (isAuditEnabled && packet.getHeader().getSrc().getPartyId().equals(myCoordinator)) {
                AUDIT.info(ToStringUtils.toOneLineString(packet));
            }

            overallStartTimestamp = System.currentTimeMillis();

            if(!proxyServerConf.getPartyId().equals(inputMetadata.getDst().getPartyId())){
                PipeHandleNotificationEvent event =
                    eventFactory.createPipeHandleNotificationEvent(
                        this, PipeHandleNotificationEvent.Type.PUSH, inputMetadata, pipe);
                applicationEventPublisher.publishEvent(event);
            } else {
                ByteString value = packet.getBody().getValue();
                String name = packet.getHeader().getTask().getModel().getName();
                String namespace = packet.getHeader().getTask().getModel().getDataKey();
                LOGGER.info("name:{}, namespace:{}", name, namespace);

                // TODO:0: better wait
                if (rollSiteUtil == null) {
                    // TODO:0: change this when delim changes
                    ErRollSiteHeader rollSiteHeader = null;
                    try {
                        rollSiteHeader = TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage(
                            RollSiteHeader.parseFrom(name.getBytes(StandardCharsets.ISO_8859_1))).fromProto();
                    } catch (InvalidProtocolBufferException e) {
                        LOGGER.error("error parsing roll site header", e);
                        onError(e);
                    }
                    int totalPartition = Integer.parseInt(rollSiteHeader.options().getOrElse(
                        StringConstants.TOTAL_PARTITIONS_SNAKECASE(), () -> "1"));
                    String job_id = rollSiteHeader.rollSiteSessionId();
                    try {
                        while (!JobStatus.isJobIdToSessionRegistered(job_id)) {
                            Thread.sleep(20);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String sessionId = JobStatus.getErSessionId(job_id);
                    LOGGER.info("ready to create rollsite util");
                    if(sessionId != null) {
                        rollSiteUtil = new RollSiteUtil(sessionId, rollSiteHeader, new Map1<>("job_id_tag", Thread.currentThread().getName()));
                    } else {
                        Throwable t = new IllegalArgumentException("session id does not exist");
                        onError(t);
                    }
                }

                if (value == null) {
                    IllegalStateException e = new IllegalStateException("value is null for name: " + name);
                    onError(e);
                    throw e;
                }

                rollSiteUtil.putBatch(value);
                // for putBatch, on complete here; for cascaded call, on complete at cascaded call
                pipe.onComplete();
                LOGGER.info("end putBatch for {}", name);
            }

            if (timeouts.isTimeout(overallTimeout, overallStartTimestamp)) {
                Throwable error = new IllegalStateException("push overall wait timeout exceeds overall timeout: " + overallTimeout
                        + ", metadata: " + oneLineStringInputMetadata);
                pipe.onError(error);
                onError(error);
                return;
            }

            if (isDebugEnabled) {
                DEBUGGING.info("[PUSH][OBSERVER][ONNEXT] server: {}, ackCount: {}", packet, ackCount.get());
                if (packet.getBody() != null && packet.getBody().getValue() != null) {
                    ByteString value = packet.getBody().getValue();
                    streamStat.increment(value.size());
                    DEBUGGING.info("[PUSH][OBSERVER][ONNEXT] length: {}, metadata: {}",
                            packet.getBody().getValue().size(), oneLineStringInputMetadata);
                } else {
                    DEBUGGING.info("[PUSH][OBSERVER][ONNEXT] length : null, metadata: {}", oneLineStringInputMetadata);
                }
                DEBUGGING.info("-------------");
            }
            LOGGER.info("push server received size: {}, data size: {}", packet.getSerializedSize(), packet.getBody().getValue().size());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.info("[PUSH][OBSERVER] onError");
        LOGGER.error("[PUSH][OBSERVER][ONERROR] error in push server: {}, metadata: {}, ackCount: {}",
                Status.fromThrowable(throwable), oneLineStringInputMetadata, ackCount.get());
        LOGGER.error(ExceptionUtils.getStackTrace(throwable));

        pipe.setDrained();

/*        if (Status.fromThrowable(throwable).getCode() != Status.Code.CANCELLED) {
            pipe.onError(throwable);
            responseObserver.onError(errorUtils.toGrpcRuntimeException(throwable));
            streamStat.onError();
        } else {
            noError = false;
            pipe.onComplete();
            LOGGER.info("[PUSH][OBSERVER][ONERROR] connection cancelled. turning into completed.");
            onCompleted();
            streamStat.onComplete();
            return;
        }*/

        pipe.onError(throwable);
        responseObserver.onError(ErrorUtils.toGrpcRuntimeException(throwable));
        streamStat.onError();
    }

    @Override
    public void onCompleted() {
        LOGGER.info("[PUSH][OBSERVER] onCompleted. ackCount: {}", ackCount);
        long lastestAckCount = ackCount.get();
        LOGGER.info("[PUSH][OBSERVER][ONCOMPLETE] trying to complete task. metadata: {}, ackCount: {}",
                oneLineStringInputMetadata, lastestAckCount);

        long completionWaitStartTimestamp = System.currentTimeMillis();
        long loopEndTimestamp = completionWaitStartTimestamp;
        long waitCount = 0;

        if (inputMetadata == null) {
            IllegalStateException e = new IllegalStateException("input metadata is null in onComplete");
            onError(e);
            throw e;
        }

        LOGGER.info("pipe in onCompleted: {}", pipe);
        pipe.setDrained();
        // pipe.onComplete();

        /*LOGGER.info("closed: {}, completion timeout: {}, overall timeout: {}",
                pipe.isClosed(),
                timeouts.isTimeout(completionWaitTimeout, completionWaitStartTimestamp, loopEnd),
                timeouts.isTimeout(overallTimeout, overallStartTimestamp, loopEnd));*/
        while (!pipe.isClosed()
                && !timeouts.isTimeout(completionWaitTimeout, completionWaitStartTimestamp, loopEndTimestamp)
                && !timeouts.isTimeout(overallTimeout, overallStartTimestamp, loopEndTimestamp)) {
            // LOGGER.info("waiting for next level result");
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
                LOGGER.info("[PUSH][OBSERVER][ONCOMPLETE] waiting push to complete. wait time: {}. metadata: {}, extrainfo: {}",
                        (loopEndTimestamp - completionWaitStartTimestamp), oneLineStringInputMetadata, extraInfo);
            }
        }
        pipe.onComplete();

        try {
            if (timeouts.isTimeout(completionWaitTimeout, completionWaitStartTimestamp, loopEndTimestamp)) {
                String errmsg = "[PUSH][OBSERVER][ONCOMPLETE] push server completion wait exceeds completionWaitTimeout. "
                        + "completionWaitTimeout: " + completionWaitTimeout
                        + ", metadata: " + oneLineStringInputMetadata
                        + ", completionWaitStartTimestamp: " + completionWaitStartTimestamp
                        + ", loopEndTimestamp: " + loopEndTimestamp
                        + ", ackCount: " + lastestAckCount;
                LOGGER.error(errmsg);
                responseObserver.onError(new TimeoutException(errmsg));
                streamStat.onError();
            } else if (timeouts.isTimeout(overallTimeout, overallStartTimestamp, loopEndTimestamp)) {
                String errmsg = "[PUSH][OBSERVER][ONCOMPLETE] push server overall time exceeds overallTimeout. "
                        + "overallTimeout: " + overallTimeout
                        + ", metadata: " + oneLineStringInputMetadata
                        + ", overallStartTimestamp: " + overallStartTimestamp
                        + ", loopEndTimestamp: " + loopEndTimestamp
                        + ", ackCount: " + lastestAckCount;

                LOGGER.error(errmsg);
                responseObserver.onError(new TimeoutException(errmsg));
                streamStat.onError();
            } else {
                /*
                Proxy.Metadata responseMetadata = pipeUtils.getResultFromPipe(pipe);
                if (responseMetadata == null) {
                    LOGGER.warn("[PUSH][OBSERVER][ONCOMPLETE] response Proxy.Metadata is null. inputMetadata: {}",
                            toStringUtils.toOneLineString(responseMetadata));
                }
                */

                //responseObserver.onNext(responseMetadata);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                LOGGER.info("[PUSH][OBSERVER][ONCOMPLETE] push server complete. inputMetadata: {}",
                        ToStringUtils.toOneLineString(response));
                streamStat.onComplete();
            }

        } catch (NullPointerException e) {
            LOGGER.error("[PUSH][OBSERVER][ONCOMPLETE] NullPointerException caught in push onComplete. metadata: {}",
                    oneLineStringInputMetadata);
        }
    }
}
