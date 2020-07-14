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

package com.webank.eggroll.rollsite.service;

import com.google.common.base.Preconditions;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.constant.CoreConfKeys;
import com.webank.eggroll.core.retry.RetryException;
import com.webank.eggroll.core.retry.Retryer;
import com.webank.eggroll.core.retry.factory.AttemptOperations;
import com.webank.eggroll.core.retry.factory.RetryerBuilder;
import com.webank.eggroll.core.retry.factory.StopStrategies;
import com.webank.eggroll.core.retry.factory.WaitTimeStrategies;
import com.webank.eggroll.core.session.StaticErConf;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent.Type;
import com.webank.eggroll.rollsite.grpc.client.DataTransferPipedClient;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.model.PipeHandlerInfo;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class CascadedCaller implements Runnable {
    @Autowired
    private DataTransferPipedClient client;

    private PipeHandlerInfo pipeHandlerInfo;

    private static final Logger LOGGER = LogManager.getLogger();

    public CascadedCaller() {
    }

    public CascadedCaller(PipeHandlerInfo pipeHandlerInfo) {
        this.pipeHandlerInfo = pipeHandlerInfo;
    }

    public void setPipeHandlerInfo(PipeHandlerInfo pipeHandlerInfo) {
        this.pipeHandlerInfo = pipeHandlerInfo;
    }

    @Override
    @Async
    public void run() {
        LOGGER.trace("cascaded calling of metadata={}", ToStringUtils.toOneLineString(pipeHandlerInfo.getMetadata()));
        Preconditions.checkNotNull(pipeHandlerInfo);
        Pipe pipe = pipeHandlerInfo.getPipe();

        Proxy.Metadata metadata = pipeHandlerInfo.getMetadata();

        Type type = pipeHandlerInfo.getType();

        if (PipeHandleNotificationEvent.Type.PUSH == type) {
            client.initPush(metadata, pipe);
            client.doPush();
            client.completePush();
        } else if (PipeHandleNotificationEvent.Type.PULL == type) {
            client.pull(metadata, pipe);
        } else {
            client.unaryCall(pipeHandlerInfo.getPacket(), pipe);
        }

    }
}
