package com.webank.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.utils.LockUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


@Singleton
public class ResourceStateHandler implements StateHandler<ErProcessor> {

    @Inject
    ProcessorResourceService processorResourceService;

    @Inject
    NodeResourceService nodeResourceService;

    @Inject
    ClusterResourceManager clusterResourceManager;

    public static ConcurrentHashMap<Long, ReentrantLock> nodeResourceLockMap = new ConcurrentHashMap<>();

    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        String stateLine = preStateParam + "_" + desStateParam;
        switch (stateLine) {
            case "init_pre_allocated":
                preAllocateResource(data);
                break;
            case "pre_allocated_allocated":
                allocatedResource(data);
                break;
            case "pre_allocated_allocate_failed":
                preAllocateFailedResource(data);
                break;
            case "allocated_return":
                returnResource(data);
                break;
        }
        this.openAsynPostHandle(context);
        return data;
    }

    private void updateResource(ErProcessor erProcessor, String desState) {
        this.processorResourceService.update(new LambdaUpdateWrapper<ProcessorResource>().set(ProcessorResource::getStatus, desState)
                .eq(ProcessorResource::getProcessorId, erProcessor.getId()));
    }

    public void asynPostHandle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        this.clusterResourceManager.countAndUpdateNodeResource(data.getServerNodeId());
    }

    public void preAllocateResource(ErProcessor erProcessor) {
        try {
//            LockUtils.lock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
            nodeResourceService.preAllocateResource(erProcessor);
            processorResourceService.preAllocateResource(erProcessor);
        }finally {
//            LockUtils.unLock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
        }
    }

    public void preAllocateFailedResource(ErProcessor erProcessor) {
        try {
//            LockUtils.lock(nodeResourceLockMap,erProcessor.getServerNodeId());
            nodeResourceService.preAllocateFailed(erProcessor);
            processorResourceService.preAllocateFailed(erProcessor);
        }finally {
//            LockUtils.unLock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
        }
    }


    public void allocatedResource(ErProcessor erProcessor) {
        try {
//            LockUtils.lock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
            nodeResourceService.allocatedResource(erProcessor);
            processorResourceService.allocatedResource(erProcessor);
        }finally {
//            LockUtils.unLock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
        }
    }

    public void returnResource(ErProcessor erProcessor) {
        try {
//            LockUtils.lock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
            nodeResourceService.returnResource(erProcessor);
            processorResourceService.returnResource(erProcessor);
        }finally {
//            LockUtils.unLock(ClusterResourceManager.sessionLockMap,erProcessor.getSessionId());
        }
    }


}