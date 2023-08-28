package com.webank.eggroll.clustermanager.processor;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.utils.CacheUtil;
import com.google.common.cache.Cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.statemachine.ProcessorStateMachine;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


@Singleton
public class DefaultProcessorManager {

    @Inject
    ProcessorStateMachine processorStateMachine;

    ConcurrentHashMap heartBeatMap = new ConcurrentHashMap<Long,ErProcessor>();

    Cache<Long,ErProcessor>  processorHeartBeat= CacheUtil.buildErProcessorCache(1000,10,TimeUnit.MINUTES);

    public  ErProcessor heartbeat(Context context, ErProcessor proc){
        ErProcessor previousHeartbeat = processorHeartBeat.asMap().get(proc.getId());
        if(previousHeartbeat==null){
            processorHeartBeat.asMap().put(proc.getId(),proc);
            processorStateMachine.changeStatus(context ,proc,null,proc.getStatus());
        }else{
            if(!previousHeartbeat.getStatus().equals(proc.getStatus())) {
                processorHeartBeat.asMap().put(proc.getId(),proc);
                processorStateMachine.changeStatus(context,proc,null,proc.getStatus());
            }
        }
        return   proc;
    };
}
