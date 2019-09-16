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

import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet;
import com.webank.eggroll.core.grpc.processor.BaseClientCallStreamProcessor;
//import com.webank.eggroll.rollsite.grpc.core.api.grpc.client.crud.BaseStreamProcessor;
import com.webank.eggroll.core.util.ToStringUtils;
//import com.webank.eggroll.rollsite.grpc.core.utils.ToStringUtils;
import com.webank.eggroll.rollsite.infra.Pipe;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import javax.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

//import com.webank.ai.eggroll.api.driver.clustercomm.ClusterComm;
//import com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet;
//import com.webank.ai.eggroll.driver.clustercomm.transfer.utils.TransferPojoUtils;
//import com.webank.ai.eggroll.driver.clustercomm.transfer.model.TransferBroker;
//import TransferBroker;
//import com.webank.ai.eggroll.rollsite.grpc.TransferProtoMessageUtils;

@Component
@Scope("prototype")
//public class PushStreamProcessor extends BaseStreamProcessor<Proxy.Packet> {
public class PushStreamProcessor extends BaseClientCallStreamProcessor<Proxy.Packet> {
    private static final Logger LOGGER = LogManager.getLogger();
    //@Autowired
    //private TransferPojoUtils transferPojoUtils;
    //@Autowired
    //private TransferProtoMessageUtils transferProtoMessageUtils;
    private Proxy.Packet.Builder packetBuilder;
    private Proxy.Data.Builder bodyBuilder;
    private Proxy.Metadata.Builder headerBuilder;
    private long seq;
    private Pipe transferBroker;
    //private ClusterComm.TransferMeta transferMeta;
    private String transferMetaString;
    private int packetCount = 0;

    private volatile boolean inited;

    public PushStreamProcessor(ClientCallStreamObserver<Packet> streamObserver, Pipe pipe) {
        super(streamObserver);
        this.transferBroker = pipe;
        //this.transferMeta = transferBroker.getTransferMeta();
        this.seq = 0;

        this.packetBuilder = Proxy.Packet.newBuilder();
        this.headerBuilder = Proxy.Metadata.newBuilder();
        this.bodyBuilder = Proxy.Data.newBuilder();
    }

    @PostConstruct
    public synchronized void init() {
        if (inited) {
            return;
        }
        /*
        headerBuilder
                .setTask(Proxy.Task.newBuilder().setTaskId(transferPojoUtils.generateTransferId(transferMeta)))
                .setSrc(transferProtoMessageUtils.partyToTopic(transferMeta.getSrc()))
                .setDst(transferProtoMessageUtils.partyToTopic(transferMeta.getDst()));

        this.transferMetaString = toStringUtils.toOneLineString(transferMeta);
        */
        inited = true;
    }

    @Override
    public void onProcess() {
        if (!inited) {
            init();
        }
        LOGGER.info("PushStreamProcessor processing");

        //super.process();

        /*
        // LOGGER.info("processing send stream for task: {}", toStringUtils.toOneLineString(transferMeta));
        List<ByteString> dataList = Lists.newLinkedList();

        int drainedCount = transferBroker.drainTo(dataList, 1000);

        if (drainedCount <= 0) {
            return;
        }

        Proxy.Packet packet = null;
        for (ByteString data : dataList) {
            headerBuilder.setSeq(++seq);
            bodyBuilder.setValue(data);

            packet = packetBuilder.setHeader(headerBuilder)
                    .setBody(bodyBuilder)
                    .build();
            streamObserver.onNext(packet);
            ++packetCount;
        }
        */

        int emptyRetryCount = 0;
        Proxy.Packet packet = null;
        do {
            //packet = (Proxy.Packet) pipe.read(1, TimeUnit.SECONDS);
            packet = (Proxy.Packet) transferBroker.read();

            if (packet != null) {
                Proxy.Metadata outputMetadata = packet.getHeader();
                Proxy.Data outData = packet.getBody();
                LOGGER.info("PushStreamProcessor processing metadata: {}", ToStringUtils.toOneLineString(outputMetadata));
                LOGGER.info("PushStreamProcessor processing outData: {}", ToStringUtils.toOneLineString(outData));

                clientCallStreamObserver.onNext(packet);
                emptyRetryCount = 0;
            } else {
                ++emptyRetryCount;
                if (emptyRetryCount % 60 == 0) {
                    //LOGGER.info("[PUSH][CLIENT] push stub waiting. empty retry count: {}, metadata: {}",
                    //   emptyRetryCount, onelineStringMetadata);
                    LOGGER.info("[PUSH][CLIENT] push stub waiting. empty retry count: {}",
                           emptyRetryCount);
                }
            }
        } while ((packet != null || !transferBroker.isDrained()) && emptyRetryCount < 30 && !transferBroker.hasError());

    }

    @Override
    public void onComplete() {
/*        LOGGER.info("[CLUSTERCOMM][PUSHPROCESSOR] trying to complete send stream for task: {}, packetCount: {}, transferBroker remaining: {}",
                transferMetaString, packetCount, transferBroker.getQueueSize());*/
/*        while (!transferBroker.isClosable()) {
            process();
        }*/

        //LOGGER.info("[CLUSTERCOMM][PUSHPROCESSOR] actual completes send stream for task: {}, packetCount: {}, transferBroker remaining: {}",
        //        transferMetaString, packetCount, transferBroker.getQueueSize());
        LOGGER.info("[CLUSTERCOMM][PUSHPROCESSOR] actual completes send stream for task: {}, packetCount: {}",
                   transferMetaString, packetCount);
        // transferBroker.setFinished();
        super.onComplete();
    }
}
