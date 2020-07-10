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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.constant.RollSiteConfKeys;
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.ErRollSiteHeader;
import com.webank.eggroll.core.meta.TransferModelPbMessageSerdes;
import com.webank.eggroll.core.transfer.Transfer.RollSiteHeader;
import com.webank.eggroll.core.util.ErrorUtils;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.RollSiteUtil;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.factory.*;
import com.webank.eggroll.rollsite.infra.JobStatus;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueueSingleResultPipe;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.service.FdnRouter;
import com.webank.eggroll.rollsite.utils.Timeouts;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import scala.Option;
import scala.Some;

@Component
@Scope("prototype")
public class DataTransferPipedServerImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {
    private static final Logger LOGGER = LogManager.getLogger(DataTransferPipedServerImpl.class);
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private ThreadPoolTaskExecutor asyncThreadPool;
    @Autowired
    private ProxyGrpcStreamObserverFactory proxyGrpcStreamObserverFactory;
    @Autowired
    private Timeouts timeouts;
    @Autowired
    private EventFactory eventFactory;
    @Autowired
    private ProxyServerConf proxyServerConf;
    @Autowired
    private ProxyGrpcStubFactory proxyGrpcStubFactory;
    private Pipe defaultPipe;
    private PipeFactory pipeFactory;
    @Autowired
    private DefaultPipeFactory defaultPipeFactory;
    @Autowired
    private FdnRouter fdnRouter;

    private static LoadingCache<String, Proxy.Packet> transferObjectCache = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .recordStats()
            .build(new CacheLoader<String, Proxy.Packet>() {
        @Override
        public Proxy.Packet load(String key) throws Exception {
            throw new IllegalStateException("loading of this cache is not supported");
        }
    });

    @Override
    public StreamObserver<Proxy.Packet> push(StreamObserver<Proxy.Metadata> responseObserver) {
        LOGGER.info("[PUSH][SERVER] request received");

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

        Pipe pipe = new PacketQueueSingleResultPipe();

        LOGGER.info("[PULL][SERVER] pull pipe: {}", pipe);

        long startTimestamp = System.currentTimeMillis();
        long lastPacketTimestamp = startTimestamp;
        long loopEndTimestamp = lastPacketTimestamp;

        Proxy.Packet packet;
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

    private void registerBroker(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Packet packet = null;

        Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();
        Proxy.Data data = Proxy.Data.newBuilder().setValue(ByteString.copyFromUtf8("hello"))
                .build();
        packet = packetBuilder.setHeader(request.getHeader())
                .setBody(data)
                .build();
        responseObserver.onNext(packet);
        responseObserver.onCompleted();
        return;
    }

    private void initJobSessionPair(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        try {
            Proxy.Packet packet = null;

            Proxy.Metadata header = request.getHeader();

            String jobId = header.getTask().getModel().getName();
            String sessionId = header.getTask().getModel().getDataKey();
            Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();
            packet = packetBuilder.setHeader(header).build();
            // TODO:1: rename job_id to federation_session_id, session_id -> eggroll_session_id
            LOGGER.info("init_job_session_pair, job_id:{}, session_id:{}", jobId, sessionId);
            JobStatus.putJobIdToSessionId(jobId, sessionId);

            responseObserver.onNext(packet);
            responseObserver.onCompleted();

            RollSiteUtil.sessionCache().get(sessionId);
        } catch (Exception e) {
                LOGGER.error("Error occured in unary call: ", e);
                responseObserver.onError(e);
        }
    }


    private void markEnd(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        try {
            Proxy.Packet packet = null;
            Proxy.Metadata header = request.getHeader();

            Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();
            packet = packetBuilder.setHeader(header).build();

            ErRollSiteHeader rollSiteHeader = restoreRollSiteHeader(
                    request.getHeader().getTask().getModel().getName());
            String tagKey = genTagKey(rollSiteHeader);
            JobStatus.addPutBatchRequiredCount(tagKey, header.getSeq());

            LOGGER.info("markEnd: {}, {}", rollSiteHeader.rollSiteSessionId(),
                    tagKey);  //obj or RollPair

            if (!JobStatus.hasLatch(tagKey)) {
                int totalPartitions = 1;

                String tpString = "1";
                Option<String> tpOption =
                        rollSiteHeader.options().get(StringConstants.TOTAL_PARTITIONS_SNAKECASE());
                if (tpOption instanceof Some) {
                    tpString = ((Some<String>) tpOption).get();
                }

                totalPartitions = Integer.parseInt(tpString);
                JobStatus.createLatch(tagKey, totalPartitions);
            }
            JobStatus.countDownFinishLatch(tagKey);
            JobStatus.setType(tagKey, rollSiteHeader.dataType());

            responseObserver.onNext(packet);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOGGER.error("Error occured in unary call: ", e);
            responseObserver.onError(e);
        }
    }

    private void getStatus(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Packet packet = null;
        Proxy.Metadata header = request.getHeader();
        String oneLineStringInputMetadata = ToStringUtils.toOneLineString(header);
        LOGGER.info("[UNARYCALL][SERVER] server unary request received. src: {}, dst: {}",
                ToStringUtils.toOneLineString(header.getSrc()),
                ToStringUtils.toOneLineString(header.getDst()));

        LOGGER.info("getStatus: {}", oneLineStringInputMetadata);
        Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();

        try {
            ErRollSiteHeader rollSiteHeader = restoreRollSiteHeader(
                    request.getHeader().getTask().getModel().getName());
            String tagKey = genTagKey(rollSiteHeader);

            long timeout = 5;
            TimeUnit unit = TimeUnit.MINUTES;
            boolean jobFinished = JobStatus.waitUntilAllCountDown(tagKey, timeout, unit)
                    && JobStatus.waitUntilPutBatchFinished(tagKey, timeout, unit);
            Proxy.Metadata resultHeader = request.getHeader();
            String type = StringConstants.EMPTY();
            if (jobFinished) {
                LOGGER.info("getStatus: job finished: {}", oneLineStringInputMetadata);
                resultHeader = Proxy.Metadata.newBuilder().setAck(123)
                        .setSrc(request.getHeader().getSrc())
                        .setDst(request.getHeader().getDst())
                        .build();
                int retryCount = 300;
                while (retryCount > 0) {
                    type = JobStatus.getType(tagKey);
                    if (!StringUtils.isBlank(type)) {
                        break;
                    }
                    Thread.sleep(50);
                    --retryCount;
                }
                JobStatus.cleanupJobStatus(tagKey);
            } else {
                LOGGER.info("getStatus: job NOT finished: {}. current latch count: {}, "
                                + "put batch required: {}, put batch finished: {}",
                        oneLineStringInputMetadata,
                        JobStatus.getFinishLatchCount(tagKey),
                        JobStatus.getPutBatchRequiredCount(tagKey),
                        JobStatus.getPutBatchFinishedCount(tagKey));
                resultHeader = Proxy.Metadata.newBuilder().setAck(321).build();
            }
            Proxy.Data body = Proxy.Data.newBuilder().setKey(tagKey)
                    .setValue(ByteString.copyFromUtf8(type)).build();
            packet = packetBuilder.setHeader(resultHeader).setBody(body).build();
            responseObserver.onNext(packet);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOGGER.error("Error occured in unary call: ", e);
            responseObserver.onError(e);
        }
    }

    private void pushObj(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        try {
            Proxy.Metadata header = request.getHeader();

            if (!proxyServerConf.getPartyId().equals(header.getDst().getPartyId())) {
                Proxy.Topic dstTopic = header.getDst();
                DataTransferServiceGrpc.DataTransferServiceBlockingStub blockstub = proxyGrpcStubFactory.getBlockingStub(dstTopic);

                Proxy.Packet ret_packet = blockstub.unaryCall(request);
                //LOGGER.info("{}", ret_packet);
                responseObserver.onNext(ret_packet);
                responseObserver.onCompleted();
            } else {
                ErRollSiteHeader rollSiteHeader = restoreRollSiteHeader(
                        request.getHeader().getTask().getModel().getName());
                String tagKey = genTagKey(rollSiteHeader);
                JobStatus.addPutBatchRequiredCount(tagKey, 1);

                LOGGER.info("received obj, tagKey: {}", tagKey);

                if (!JobStatus.hasLatch(tagKey)) {
                    int totalPartitions = 1;

                    String tpString = "1";
                    Option<String> tpOption =
                            rollSiteHeader.options().get(StringConstants.TOTAL_PARTITIONS_SNAKECASE());
                    if (tpOption instanceof Some) {
                        tpString = ((Some<String>) tpOption).get();
                    }

                    totalPartitions = Integer.parseInt(tpString);
                    JobStatus.createLatch(tagKey, totalPartitions);
                }
                JobStatus.countDownFinishLatch(tagKey);
                JobStatus.setType(tagKey, rollSiteHeader.dataType());

                transferObjectCache.put(tagKey, request);

                JobStatus.increasePutBatchFinishedCount(tagKey);

                responseObserver.onNext(request);
                responseObserver.onCompleted();
            }
        } catch (Exception e) {
            LOGGER.error("Error occured in unary call: ", e);
            responseObserver.onError(e);
        }
    }

    private void pullObj(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Metadata header = request.getHeader();

        String tagKey = header.getDst().getName();
        Proxy.Packet ret = transferObjectCache.getIfPresent(tagKey);
        transferObjectCache.invalidate(tagKey);
        responseObserver.onNext(ret);
        responseObserver.onCompleted();
    }

    private void getRouteTable(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Metadata header = request.getHeader();
        Proxy.Packet packet = null;
        String jsonContent = null;
        Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();

        if (!proxyServerConf.getPartyId().equals(header.getDst().getPartyId())) {
            throw new IllegalArgumentException("dst partyId is illegal.");
        } else {
            String routeTablePath = proxyServerConf.getRouteTablePath();
            String srcIp = (String) AddrAuthServerInterceptor.REMOTE_ADDR.get();

            if (srcIp == null) {
                throw new IllegalArgumentException("srcIp cannot be null");
            } else {
                String[] whiteList = proxyServerConf.getWhiteList();
                if (whiteList != null && Arrays.asList(whiteList).contains(srcIp)) {
                    File jsonFile = new File(routeTablePath);
                    try {
                        String encoding = "UTF-8";
                        Long filelength = jsonFile.length();
                        byte[] filecontent = new byte[filelength.intValue()];
                        FileInputStream in = new FileInputStream(jsonFile);
                        in.read(filecontent);
                        jsonContent = new String(filecontent, encoding);
                    } catch (IOException e) {
                        LOGGER.error("getRouteTable failed: ", e);
                        responseObserver.onError(e);
                    }
                }
            }
            if(jsonContent != null) {
                Proxy.Data body = Proxy.Data.newBuilder().setValue(ByteString.copyFromUtf8(jsonContent)).build();
                packet = packetBuilder.setBody(body).build();
            } else {
                packet = packetBuilder.build();
            }
            responseObserver.onNext(packet);
            responseObserver.onCompleted();
        }
    }

    private void setRouteTable(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Metadata header = request.getHeader();
        if (!proxyServerConf.getPartyId().equals(header.getDst().getPartyId())) {
            throw new IllegalArgumentException("dst partyId is illegal.");
        } else {
            String routeTablePath = proxyServerConf.getRouteTablePath();
            String srcIp = (String) AddrAuthServerInterceptor.REMOTE_ADDR.get();

            if (srcIp == null) {
                throw new IllegalArgumentException("srcIp cannot be null");
            } else {
                String[] whiteList = proxyServerConf.getWhiteList();
                if (whiteList != null && Arrays.asList(whiteList).contains(srcIp)) {
                    String jsonString = request.getBody().getValue().toStringUtf8();
                    //String jsonString = format(result.toString());
                    try {
                        System.out.println("routeTablePath:" + routeTablePath);
                        File file = new File(routeTablePath);
                        if (!file.getParentFile().exists()) {
                            file.getParentFile().mkdirs();
                        }
                        if (file.exists()) {
                            file.delete();
                        }
                        file.createNewFile();

                        Writer write = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
                        write.write(jsonString);
                        write.flush();
                        write.close();
                        fdnRouter.updateRouteTable();
                    } catch (Exception e) {
                        LOGGER.error("setRouteTable failed: ", e);
                        responseObserver.onError(e);
                    }
                }
            }

            Proxy.Packet packet = null;

            Proxy.Packet.Builder packetBuilder = Proxy.Packet.newBuilder();
            Proxy.Data data = Proxy.Data.newBuilder().setValue(ByteString.copyFromUtf8("setRouteTable finished"))
                    .build();
            packet = packetBuilder.setBody(data).build();
            responseObserver.onNext(packet);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void unaryCall(Proxy.Packet request, StreamObserver<Proxy.Packet> responseObserver) {
        Proxy.Packet packet = null;
        boolean hasReturnedBefore = false;
        int emptyRetryCount = 0;

        //long completionWaitTimeout = Timeouts.DEFAULT_COMPLETION_WAIT_TIMEOUT;
        long overallTimeout = Timeouts.DEFAULT_OVERALL_TIMEOUT;
        long packetIntervalTimeout = Timeouts.DEFAULT_PACKET_INTERVAL_TIMEOUT;

        Proxy.Metadata header = request.getHeader();
        String oneLineStringInputMetadata = ToStringUtils.toOneLineString(header);
        LOGGER.info("[UNARYCALL][SERVER] server unary request received. src: {}, dst: {}",
                ToStringUtils.toOneLineString(header.getSrc()),
                ToStringUtils.toOneLineString(header.getDst()));

        if (header.hasConf()) {
            overallTimeout = timeouts.getOverallTimeout(header);
            packetIntervalTimeout = timeouts.getPacketIntervalTimeout(header);
        }

        LOGGER.info("taskId:{}", header.getTask().getTaskId());

        try {
            if (header.getOperator().equals("registerBroker")) {
                registerBroker(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("init_job_session_pair")) {
                initJobSessionPair(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("markEnd")
                && proxyServerConf.getPartyId().equals(header.getDst().getPartyId())) {
                markEnd(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("getStatus")) {
                getStatus(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("push_obj")) {
                pushObj(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("pull_obj")) {
                pullObj(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("get_route_table")) {
                getRouteTable(request, responseObserver);
                return;
            }

            if (header.getOperator().equals("set_route_table")) {
                setRouteTable(request, responseObserver);
                return;
            }

            Pipe pipe = new PacketQueueSingleResultPipe();
            LOGGER.info("self send: {}", ByteString.copyFromUtf8(pipe.getType()));
            PipeHandleNotificationEvent event =
                eventFactory.createPipeHandleNotificationEvent(
                    this, PipeHandleNotificationEvent.Type.UNARY_CALL, request, pipe);
/*            CascadedCaller cascadedCaller = applicationContext.getBean(CascadedCaller.class, event.getPipeHandlerInfo());
            asyncThreadPool.submit(cascadedCaller);*/
            applicationEventPublisher.publishEvent(event);

            long startTimestamp = System.currentTimeMillis();
            long lastPacketTimestamp = startTimestamp;
            long loopEndTimestamp = System.currentTimeMillis();
            long maxRetryCount = Long.parseLong(RollSiteConfKeys.EGGROLL_ROLLSITE_UNARYCALL_MAX_RETRY().get());
            while ((!hasReturnedBefore || !pipe.isDrained())
                && !pipe.hasError()
                && emptyRetryCount < maxRetryCount
                && !timeouts.isTimeout(overallTimeout, startTimestamp, loopEndTimestamp)) {
                packet = (Proxy.Packet) pipe.read(1, TimeUnit.SECONDS);
//            packet = request;
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
                        LOGGER.info(
                            "[UNARYCALL][SERVER] unary call waiting. current overallWaitTime: {}, packetIntervalTimeout: {}, metadata: {}",
                            currentOverallWaitTime, packetIntervalTimeout,
                            oneLineStringInputMetadata);
                    }
                }
            }
            boolean hasError = true;

            if (pipe.hasError()) {
                Throwable error = pipe.getError();
                LOGGER.error("[UNARYCALL][SERVER] unary call finish with error: {}",
                    ExceptionUtils.getStackTrace(error));
                responseObserver.onError(error);

                return;
            }

            if (!hasReturnedBefore) {
                if (timeouts.isTimeout(overallTimeout, startTimestamp, loopEndTimestamp)) {
                    String errorMsg =
                        "[UNARYCALL][SERVER] unary call server error: overall process time exceeds timeout: "
                            + overallTimeout
                            + ", metadata: " + oneLineStringInputMetadata
                            + ", overallTimeout: " + overallTimeout
                            + ", lastPacketTimestamp: " + lastPacketTimestamp
                            + ", loopEndTimestamp: " + loopEndTimestamp;
                    LOGGER.error(errorMsg);

                    TimeoutException e = new TimeoutException(errorMsg);
                    responseObserver.onError(ErrorUtils.toGrpcRuntimeException(e));
                    pipe.onError(e);
                } else {
                    String errorMsg =
                        "[UNARYCALL][SERVER] unary call server error: overall process time exceeds timeout: "
                            + overallTimeout
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

            LOGGER.info(
                "[UNARYCALL][SERVER] server unary call completed. hasReturnedBefore: {}, hasError: {}, metadata: {}",
                hasReturnedBefore, hasError, oneLineStringInputMetadata);
        } catch (Exception e) {
            LOGGER.error("Error occured in unary call: ", e);
            responseObserver.onError(e);
        }
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

    private ErRollSiteHeader restoreRollSiteHeader(String s)
        throws InvalidProtocolBufferException {
        return TransferModelPbMessageSerdes.ErRollSiteHeaderFromPbMessage(
            RollSiteHeader.parseFrom(s.getBytes(StandardCharsets.ISO_8859_1))).fromProto();
    }

    private String genTagKey(ErRollSiteHeader rollSiteHeader) {
        return rollSiteHeader
            .concat(StringConstants.HASH(), new String[]{"__federation__"});
    }

    public void setDefaultPipe(Pipe defaultPipe) {
        this.defaultPipe = defaultPipe;
    }

    public void setPipeFactory(PipeFactory pipeFactory) {
        this.pipeFactory = pipeFactory;
    }
}
