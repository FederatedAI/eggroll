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

import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.event.model.PipeHandleNotificationEvent;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueuePipe;
import com.webank.eggroll.rollsite.model.PipeHandlerInfo;
import com.webank.eggroll.rollsite.service.CascadedCaller;
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
    @Autowired
    private ApplicationContext applicationContext;
//    @Autowired
//    private ThreadPoolTaskExecutor asyncThreadPool;

    @Override
    public void onApplicationEvent(PipeHandleNotificationEvent pipeHandleNotificationEvent) {
        // LOGGER.warn("event listened: {}", pipeHandleNotificationEvent.getPipeHandlerInfo());
        LOGGER.trace("event metadata={}", ToStringUtils.toOneLineString(pipeHandleNotificationEvent.getPipeHandlerInfo().getMetadata()));

        PipeHandlerInfo pipeHandlerInfo = pipeHandleNotificationEvent.getPipeHandlerInfo();
        Pipe pipe = pipeHandlerInfo.getPipe();

        if (pipe instanceof PacketQueuePipe) {
            CascadedCaller cascadedCaller = applicationContext.getBean(CascadedCaller.class, pipeHandlerInfo);
            //asyncThreadPool.submit(cascadedCaller);
            cascadedCaller.run();
        }
    }
}

/*
@Component
@Scope("prototype")
public class PipeHandleNotificationEventListener implements ApplicationListener<PipeHandleNotificationEvent> {
    private static final Logger LOGGER = LogManager.getLogger(PipeHandleNotificationEventListener.class);
    //@Autowired
    //private ApplicationContext applicationContext;
    @Autowired
    private ThreadPoolTaskExecutor asyncThreadPool;

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
                */
/*
                client.initPush(metadata, pipe);
                client.doPush();
                client.completePush();
                *//*

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
            .getLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS(), 10000L);
        int maxAttempts = StaticErConf
            .getInt(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS(), 10);
        long attemptTimeout = StaticErConf
            .getLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS(), 30000L);

        // TODO:0: configurable
        Retryer<Integer> retryer = RetryerBuilder.<Integer>newBuilder()
            .withWaitTimeStrategy(WaitTimeStrategies.fixedWaitTime(fixedWaitTime))
            .withStopStrategy(StopStrategies.stopAfterMaxAttempt(maxAttempts))
            .withAttemptOperation(
                AttemptOperations.<Integer>fixedTimeLimit(attemptTimeout, TimeUnit.MINUTES))
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
*/
