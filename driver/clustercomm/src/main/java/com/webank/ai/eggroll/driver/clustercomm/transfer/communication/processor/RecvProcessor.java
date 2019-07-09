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

package com.webank.ai.eggroll.driver.clustercomm.transfer.communication.processor;

import com.webank.ai.eggroll.api.driver.clustercomm.ClusterComm;
import com.webank.ai.eggroll.driver.clustercomm.factory.TransferServiceFactory;
import com.webank.ai.eggroll.driver.clustercomm.transfer.communication.action.TransferQueueConsumeAction;
import com.webank.ai.eggroll.driver.clustercomm.transfer.communication.consumer.TransferBrokerConsumer;
import com.webank.ai.eggroll.driver.clustercomm.transfer.manager.RecvBrokerManager;
import com.webank.ai.eggroll.driver.clustercomm.transfer.model.TransferBroker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Scope("prototype")
public class RecvProcessor extends BaseTransferProcessor {
    private static final Logger LOGGER = LogManager.getLogger();
    @Autowired
    private RecvBrokerManager recvBrokerManager;
    @Autowired
    private TransferServiceFactory transferServiceFactory;

    public RecvProcessor(ClusterComm.TransferMeta transferMeta) {
        super(transferMeta);
    }

    @Override
    public void run() {
        try {
            LOGGER.info("[CLUSTERCOMM][RECV][PROCESSOR] transferMetaId: {}", transferMetaId);
            String transferMetaId = transferPojoUtils.generateTransferId(transferMeta);

            TransferBroker transferBroker = recvBrokerManager.getBroker(transferMetaId);
            if (transferBroker == null) {
                transferBroker = recvBrokerManager.createIfNotExists(transferMeta);

/*                if (transferBroker == null) {
                    transferBroker = recvBrokerManager.getBroker(transferMetaId);
                }*/
            }

            transferBroker = recvBrokerManager.getBroker(transferMetaId);
            LOGGER.info("[CLUSTERCOMM][RECV][PROCESSOR] broker: {}, transferMetaId: {}", transferBroker, transferMetaId);

            //LOGGER.info("recv async broker status before: {}", transferBroker.getBrokerStatus().name());
            if (transferBroker.getTransferMeta() == null) {
                transferBroker.setTransferMeta(transferMeta);
            }
            //LOGGER.info("recv async broker status after: {}", transferBroker.getBrokerStatus().name());

            TransferQueueConsumeAction recvConsumeAction = null;

            ClusterComm.TransferDataType transferDataType = transferMeta.getDataDesc().getTransferDataType();

//            LOGGER.info("transferType: {}", transferDataType == null ? "null" : transferDataType.name());

            // if transfer data type is not specified in recv request, then use passed in type from send side
            if (transferDataType == null || transferDataType == ClusterComm.TransferDataType.NOT_SPECIFIED) {
                // todo: make this configurable
                ClusterComm.TransferMeta passedInTransferMeta = recvBrokerManager.blockingGetPassedInTransferMeta(transferMetaId,
                        1, TimeUnit.DAYS);
                if (passedInTransferMeta == null) {
                    throw new TimeoutException("time exceeds when waiting for send request");
                }
                transferDataType = passedInTransferMeta.getDataDesc().getTransferDataType();
            }

            switch (transferDataType) {
                case DTABLE:
                    recvConsumeAction = transferServiceFactory.createDtableRecvConsumeAction(transferBroker);
                    break;
                case OBJECT:
                    recvConsumeAction = transferServiceFactory.createObjectRecvConsumeLmdbAction(transferBroker);
                    break;
                default:
                    throw new UnsupportedOperationException("illegal transfer type for transfermeta: " + toStringUtils.toOneLineString(transferMeta));
            }

            transferBroker.setAction(recvConsumeAction);

            TransferBrokerConsumer consumer = transferServiceFactory.createTransferBrokerConsumer();
            transferBroker.addSubscriber(consumer);

            ioConsumerPool.submit(consumer);

            consumer.onListenerChange(transferBroker);

            LOGGER.info("[CLUSTERCOMM][RECV][PROCESSOR] broker isFinished: {}", transferBroker.isFinished());
        } catch (Exception e) {
            LOGGER.error(errorUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }
}
