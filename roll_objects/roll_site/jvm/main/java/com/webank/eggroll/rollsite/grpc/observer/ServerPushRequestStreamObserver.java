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
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.util.ErrorUtils;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.factory.EventFactory;
import com.webank.eggroll.rollsite.factory.PipeFactory;
import com.webank.eggroll.rollsite.helper.ModelValidationHelper;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueuePipe;
import com.webank.eggroll.rollsite.manager.StatsManager;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.model.StreamStat;
import com.webank.eggroll.rollsite.utils.PipeUtils;
import com.webank.eggroll.rollsite.utils.Timeouts;
import io.grpc.Grpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
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
    private PipeUtils pipeUtils;
    private Pipe pipe;
    PipeFactory pipeFactory;
    //Map<String, PacketQueueSingleResultPipe> pipeMap;
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

        //Pipe pipe = getPipe("modelA");

        //if (inputMetadata == null) {
        //overallStartTimestamp = System.currentTimeMillis();
            inputMetadata = packet.getHeader();
            pipe = pipeFactory.create(inputMetadata.getTask().getModel().getName());

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

            // String operator = inputMetadata.getOperator();

            // LOGGER.info("onNext(): push task name: {}", operator);
            /*
            overallStartTimestamp = System.currentTimeMillis();
            if(proxyServerConf.getPartyId() != Integer.valueOf(inputMetadata.getDst().getPartyId())) {
                //if(Integer.valueOf(inputMetadata.getDst().getPartyId()))
                PipeHandleNotificationEvent event =
                    eventFactory.createPipeHandleNotificationEvent(
                        this, PipeHandleNotificationEvent.Type.PUSH, inputMetadata, pipe);
                applicationEventPublisher.publishEvent(event);
            }
            */

        //}


        LOGGER.info("model name: {}", inputMetadata.getTask().getModel().getName());


        if (noError) {
            pipe.write(packet);
            ackCount.incrementAndGet();
            //LOGGER.info("myCoordinator: {}, Proxy.Packet coordinator: {}", myCoordinator, packet.getHeader().getSrc().getCoordinator());
            if (isAuditEnabled && packet.getHeader().getSrc().getPartyId().equals(myCoordinator)) {
                AUDIT.info(ToStringUtils.toOneLineString(packet));
            }

            overallStartTimestamp = System.currentTimeMillis();

            if(!proxyServerConf.getPartyId().equals(inputMetadata.getDst().getPartyId())){
                //if(Integer.valueOf(inputMetadata.getDst().getPartyId()))
                PipeHandleNotificationEvent event =
                    eventFactory.createPipeHandleNotificationEvent(
                        this, PipeHandleNotificationEvent.Type.PUSH, inputMetadata, pipe);
                applicationEventPublisher.publishEvent(event);
            }


            if (timeouts.isTimeout(overallTimeout, overallStartTimestamp)) {
                onError(new IllegalStateException("push overall wait timeout exceeds overall timeout: " + overallTimeout
                        + ", metadata: " + oneLineStringInputMetadata));
                pipe.close();
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

        if(proxyServerConf.getPartyId().equals(inputMetadata.getDst().getPartyId())) {
            pipe.setDrained();
            pipe.onComplete();
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
        LOGGER.info("[PUSH][OBSERVER] onCompleted");
        long lastestAckCount = ackCount.get();
        LOGGER.info("[PUSH][OBSERVER][ONCOMPLETE] trying to complete task. metadata: {}, ackCount: {}",
                oneLineStringInputMetadata, lastestAckCount);

        long completionWaitStartTimestamp = System.currentTimeMillis();
        long loopEndTimestamp = completionWaitStartTimestamp;
        long waitCount = 0;

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
                //LOGGER.info("[PUSH][OBSERVER][ONCOMPLETE] waiting push to complete. wait time: {}. metadata: {}, extrainfo: {}",
                //        (loopEndTimestamp - completionWaitStartTimestamp), oneLineStringInputMetadata, extraInfo);


            }
        }

        //pipe.onComplete();

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
