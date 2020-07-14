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

package com.webank.eggroll.rollsite.manager;

import com.webank.ai.eggroll.api.core.BasicMeta.Job;
import com.webank.eggroll.core.transfer.GrpcClientUtils;
import com.webank.eggroll.rollsite.RollSiteUtil;
import com.webank.eggroll.rollsite.factory.ProxyGrpcStubFactory;
import com.webank.eggroll.rollsite.infra.JobStatus;
import io.grpc.Grpc;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component
public class StatsLogger {
    private static final String LOG_TEMPLATE;
    private static final Logger LOGGER = LogManager.getLogger("stat");

    static {
        LOG_TEMPLATE = "executor poolName=${name}, " +
                "size=${size}, " +
                "activeCount=${activeCount}";
    }

    @Autowired
    private ThreadPoolTaskExecutor asyncThreadPool;
    @Autowired
    private ThreadPoolTaskExecutor grpcServiceExecutor;
    @Autowired
    private ThreadPoolTaskExecutor grpcClientExecutor;
    @Autowired
    private ThreadPoolTaskScheduler routineScheduler;
    @Autowired
    private ProxyGrpcStubFactory proxyGrpcStubFactory;

    private List<ThreadPoolTaskExecutor> threadPoolExecutors;

    public StatsLogger() {
        threadPoolExecutors = new LinkedList<>();
    }

    @PostConstruct
    private void init() {
        threadPoolExecutors.add(asyncThreadPool);
        threadPoolExecutors.add(grpcServiceExecutor);
        threadPoolExecutors.add(grpcClientExecutor);
    }

    public String logStats() {
        Map<String, String> valuesMap = new HashMap<>(10);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(valuesMap);
        StringBuilder builder = new StringBuilder();

        for (ThreadPoolTaskExecutor executor : threadPoolExecutors) {
            valuesMap.clear();
            valuesMap.put("name", executor.getThreadNamePrefix().replace("-", ""));
            valuesMap.put("size", String.valueOf(executor.getPoolSize()));
            valuesMap.put("activeCount", String.valueOf(executor.getActiveCount()));

            String log = stringSubstitutor.replace(LOG_TEMPLATE);
            builder.append(log)
                .append("; ");
        }

        builder.append("jobIcToSessionId.size=")
            .append(JobStatus.getJobIdToSessionId().size());

        builder.append("; jobIdToFinishLatch.size=")
            .append(JobStatus.getJobIdToFinishLatch().size());

        builder.append("; jobIdToPutBatchRequiredCount.size=")
            .append(JobStatus.getJobIdToPutBatchRequiredCount().size());

        builder.append("; jobIdToPutBatchFinishedCount.size=")
            .append(JobStatus.getJobIdToPutBatchFinishedCount().size());

        builder.append("; tagkeyToObjType.size=")
            .append(JobStatus.getTagkeyToObjType().size());

        builder.append("; erSessionCache.size")
            .append(RollSiteUtil.sessionCache().size());

        builder.append("; GrpcClientUtils.insecureChannelCache.size=")
            .append(GrpcClientUtils.getChannelCacheSize(false))
            .append("; GrpcClientUtils.secureChannelCache.size=")
            .append(GrpcClientUtils.getChannelCacheSize(true));

        builder.append("; ProxyGrpcStubFactory.channelCache.size=")
            .append(proxyGrpcStubFactory.getChannelCacheSize());

        String finalLog = builder.toString();
        LOGGER.info(finalLog);

        return finalLog;
    }
}
