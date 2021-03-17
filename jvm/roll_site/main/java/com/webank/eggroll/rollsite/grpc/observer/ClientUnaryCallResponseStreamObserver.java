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
import com.webank.eggroll.rollsite.utils.ToAuditString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope("prototype")
public class ClientUnaryCallResponseStreamObserver implements StreamObserver<Proxy.Packet> {
    private static final Logger LOGGER = LogManager.getLogger(ClientUnaryCallResponseStreamObserver.class);
    private static final Logger DEBUGGING = LogManager.getLogger("debugging");
    private static final Logger AUDIT = LogManager.getLogger("audit");
    private final CountDownLatch finishLatch;
    @Autowired
    private StatsManager statsManager;
    @Autowired
    private ProxyServerConf proxyServerConf;
    private StreamStat streamStat;
    private Proxy.Metadata metadata;
    private Pipe pipe;
    private boolean isInited;
    private AtomicLong ackCount;

    public ClientUnaryCallResponseStreamObserver(Pipe pipe, CountDownLatch finishLatch, Proxy.Metadata metadata) {
        this.finishLatch = finishLatch;
        this.pipe = pipe;
        this.metadata = metadata;

        this.streamStat = new StreamStat(metadata, StreamStat.UNARY_CALL);
        this.ackCount = new AtomicLong(0L);
    }

    @PostConstruct
    private synchronized void init() {
        if (isInited) {
            return;
        }

        statsManager.add(streamStat);
        isInited = true;
    }

    @Override
    public void onNext(Proxy.Packet packet) {
        // LOGGER.info("ClientPullResponseStreamObserver.onNext()");
        pipe.write(packet);
        ackCount.incrementAndGet();

        if (!isInited) {
            init();
        }

/*        if (proxyServerConf.isDebugEnabled()) {
            DEBUGGING.info("[UNARYCALL][OBSERVER][ONNEXT]: {}", packet);
            DEBUGGING.info("-------------");
        }*/

        if (proxyServerConf.isAuditEnabled()
                && packet.getHeader().getSrc().getPartyId().equals(proxyServerConf.getCoordinator())) {
            AUDIT.info(ToStringUtils.toOneLineString(packet));
        }

        String[] auditTopics = proxyServerConf.getAuditTopics();
        if (auditTopics != null
                && (Arrays.asList(auditTopics).contains(packet.getHeader().getSrc().getRole())
                    || Arrays.asList(auditTopics).contains(packet.getHeader().getDst().getRole()))){
            AUDIT.info(ToAuditString.toOneLineString(packet.getHeader(), "|"));
        }

        if (packet.getBody() != null && packet.getBody().getValue() != null) {
            ByteString value = packet.getBody().getValue();
            streamStat.increment(value.size());
        }
        // LOGGER.info("[UNARYCALL][OBSERVER][ONNEXT] result: {}", packet.getBody().getValue().toStringUtf8());
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("[UNARYCALL][OBSERVER][ONERROR] error in unary call response observer: {}, metadata: {}",
                Status.fromThrowable(throwable), ToStringUtils.toOneLineString(metadata));
        LOGGER.error(ExceptionUtils.getStackTrace(throwable));

        pipe.onError(throwable);

        streamStat.onError();
        finishLatch.countDown();
    }

    @Override
    public void onCompleted() {
        LOGGER.trace("[UNARYCALL][OBSERVER][ONCOMPLETE] Client unary call completed. metadata: {}",
                ToStringUtils.toOneLineString(metadata));

        pipe.onComplete();
        finishLatch.countDown();
        streamStat.onComplete();
    }

    public CountDownLatch getFinishLatch() {
        return this.finishLatch;
    }
}
