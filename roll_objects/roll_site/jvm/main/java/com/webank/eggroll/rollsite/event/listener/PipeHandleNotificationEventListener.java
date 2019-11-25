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

package com.webank.eggroll.rollsite.event.listener;

import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.constant.CoreConfKeys;
import com.webank.eggroll.core.meta.ErEndpoint;
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
import com.webank.eggroll.rollsite.infra.impl.PacketQueuePipe;
import com.webank.eggroll.rollsite.model.PipeHandlerInfo;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class PipeHandleNotificationEventListener implements ApplicationListener<PipeHandleNotificationEvent> {
    private static final Logger LOGGER = LogManager.getLogger(PipeHandleNotificationEventListener.class);
    //@Autowired
    //private ApplicationContext applicationContext;
    @Autowired
    private DataTransferPipedClient client;

    @Override
    public void onApplicationEvent(PipeHandleNotificationEvent pipeHandleNotificationEvent) {
        // LOGGER.warn("event listened: {}", pipeHandleNotificationEvent.getPipeHandlerInfo());
        LOGGER.info("event metadata: {}", ToStringUtils.toOneLineString(pipeHandleNotificationEvent.getPipeHandlerInfo().getMetadata()));

        PipeHandlerInfo pipeHandlerInfo = pipeHandleNotificationEvent.getPipeHandlerInfo();
        Pipe pipe = pipeHandlerInfo.getPipe();
        if (pipe instanceof PacketQueuePipe) {
            //CascadedCaller cascadedCaller = applicationContext.getBean(CascadedCaller.class, pipeHandlerInfo);
            //cascadedCaller.run();
            Proxy.Metadata metadata = pipeHandlerInfo.getMetadata();

            Type type = pipeHandlerInfo.getType();

            if (PipeHandleNotificationEvent.Type.PUSH == type) {
                /*
                client.initPush(metadata, pipe);
                client.doPush();
                client.completePush();
                */
                pushStream(metadata, pipe);

            } else if (PipeHandleNotificationEvent.Type.PULL == type) {
                client.pull(metadata, pipe);
            } else {
                client.unaryCall(pipeHandlerInfo.getPacket(), pipe);
            }
        }

    }


    public int pushStream(final Proxy.Metadata metadata, Pipe pipe) {
        int result = 0;
        long fixedWaitTime = StaticErConf
            .getLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS(), 1000L);
        int maxAttempts = StaticErConf
            .getInt(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS(), 10);
        long attemptTimeout = StaticErConf
            .getLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS(), 3000L);

        Retryer<Integer> retryer = RetryerBuilder.<Integer>newBuilder()
            .withWaitTimeStrategy(WaitTimeStrategies.fixedWaitTime(fixedWaitTime))
            .withStopStrategy(StopStrategies.stopAfterMaxAttempt(maxAttempts))
            .withAttemptOperation(
                AttemptOperations.<Integer>fixedTimeLimit(attemptTimeout, TimeUnit.MILLISECONDS))
            .retryIfAnyException()
            .build();

        final Callable<Integer> pushStreamRetry = () -> pushStreamInternal(metadata,
            pipe);

        try {
            result = retryer.call(pushStreamRetry);
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (RetryException e) {
            LOGGER.error("Error getting ManagedChannel after retries");
        }

        return result;
    }

    int pushStreamInternal(Proxy.Metadata metadata, Pipe pipe) {
        client.initPush(metadata, pipe);
        client.doPush();
        client.completePush();
        //int ret = client.getResult();
        return 0;
    }
}
