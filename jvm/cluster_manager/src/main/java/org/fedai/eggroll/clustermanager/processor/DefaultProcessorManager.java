package org.fedai.eggroll.clustermanager.processor;

import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.fedai.eggroll.core.utils.CacheUtil;
import com.google.common.cache.Cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.statemachine.ProcessorStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.TimeUnit;


@Singleton
public class DefaultProcessorManager {

    Logger logger = LoggerFactory.getLogger(DefaultProcessorManager.class);
    @Inject
    ProcessorStateMachine processorStateMachine;
    Cache<String, ErProcessor> processorHeartBeat = CacheUtil.buildErProcessorCache(1000, 1, TimeUnit.MINUTES);

    public ErProcessor heartbeat(Context context, ErProcessor proc) {
        ErProcessor previousHeartbeat = processorHeartBeat.asMap().get(proc.getId().toString());
        if (previousHeartbeat == null) {
            processorHeartBeat.asMap().put(proc.getId().toString(), proc);
            processorStateMachine.changeStatus(context, proc, null, proc.getStatus());
        } else {
            if (!previousHeartbeat.getStatus().equals(proc.getStatus())) {
                processorHeartBeat.asMap().put(proc.getId().toString(), proc);
                processorStateMachine.changeStatus(context, proc, null, proc.getStatus());
            }
        }
        return proc;
    }

    ;


}
