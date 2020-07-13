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
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.manager.StatsManager;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.model.StreamStat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope("prototype")
public class ClientPullResponseStreamObserver implements StreamObserver<Proxy.Packet> {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Logger AUDIT = LogManager.getLogger("audit");
    private static final Logger DEBUGGING = LogManager.getLogger("debugging");
    private final CountDownLatch finishLatch;
    @Autowired
    private StatsManager statsManager;
    @Autowired
    private ProxyServerConf proxyServerConf;
    private StreamStat streamStat;
    private volatile boolean isStreamStatSet;
    private Proxy.Metadata metadata;
    private String oneLineStringMetadata;
    private Pipe pipe;
    private String myCoordinator;
    private boolean isAuditEnabled;
    private boolean isDebugEnabled;
    private boolean isInited;
    private AtomicLong ackCount;

    public ClientPullResponseStreamObserver(Pipe pipe, CountDownLatch finishLatch, Proxy.Metadata metadata) {
        this.finishLatch = finishLatch;
        this.pipe = pipe;
        this.isStreamStatSet = false;
        this.metadata = metadata;

        this.streamStat = new StreamStat(metadata, StreamStat.PULL);
        this.ackCount = new AtomicLong(0L);
    }

    @PostConstruct
    private synchronized void init() {
        if (isInited) {
            return;
        }
        this.oneLineStringMetadata = ToStringUtils.toOneLineString(metadata);

        statsManager.add(streamStat);

        if (streamStat != null) {
            this.isStreamStatSet = true;
        }

        if (StringUtils.isBlank(myCoordinator)) {
            myCoordinator = proxyServerConf.getCoordinator();
        }

        isAuditEnabled = proxyServerConf.isAuditEnabled();
        isDebugEnabled = proxyServerConf.isDebugEnabled();

        isInited = true;
    }

    @Override
    public void onNext(Proxy.Packet packet) {
        pipe.write(packet);
        ackCount.incrementAndGet();

        if (!isInited) {
            init();
        }

        if (isAuditEnabled && packet.getHeader().getSrc().getPartyId().equals(myCoordinator)) {
            AUDIT.info(ToStringUtils.toOneLineString(packet));
        }

        if (isDebugEnabled) {
            DEBUGGING.info("[PULL][OBSERVER][ONNEXT] pull: {}, ackCount: {}", packet, ackCount.get());
            if (packet.getBody() != null && packet.getBody().getValue() != null) {
                ByteString value = packet.getBody().getValue();
                streamStat.increment(value.size());
                DEBUGGING.info("[PULL][OBSERVER][ONNEXT] length: {}, metadata: {}",
                        packet.getBody().getValue().size(), oneLineStringMetadata);
            } else {
                DEBUGGING.info("[PULL][OBSERVER][ONNEXT] length: null, metadata: {}", oneLineStringMetadata);
            }
            DEBUGGING.info("-------------");
        }
        if (packet.getBody() != null && packet.getBody().getValue() != null) {
            ByteString value = packet.getBody().getValue();
            streamStat.increment(value.size());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("[PULL][OBSERVER][ONERROR] error in pull client: {}, metadata: {}, ackCount: {}",
                Status.fromThrowable(throwable), oneLineStringMetadata, ackCount.incrementAndGet());
        LOGGER.error(ExceptionUtils.getStackTrace(throwable));

        pipe.onError(throwable);

        finishLatch.countDown();
        streamStat.onError();
    }

    @Override
    public void onCompleted() {
        long latestAckCount = ackCount.get();
        LOGGER.debug("[PULL][OBSERVER][ONCOMPLETE] Client pull completed. metadata: {}, ackCount: {}",
                oneLineStringMetadata, latestAckCount);

        pipe.onComplete();
        finishLatch.countDown();

        try {
            LOGGER.debug("[PULL][OBSERVER][ONCOMPLETE] is streamStat set: {}", isStreamStatSet);
            if (streamStat != null) {
                streamStat.onComplete();
            }
        } catch (NullPointerException e) {
            LOGGER.error("[PULL][OBSERVER][ONCOMPLETE] NullPointerException caught in pull onComplete. isStreamStatSet: {}", isStreamStatSet);
        }
    }

    public CountDownLatch getFinishLatch() {
        return this.finishLatch;
    }
}
