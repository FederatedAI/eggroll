package org.fedai.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.fedai.eggroll.clustermanager.cluster.ClusterResourceManager;
import org.fedai.eggroll.clustermanager.entity.ProcessorResource;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.fedai.eggroll.core.pojo.ErResource;
import org.fedai.eggroll.core.utils.LockUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.NodeResourceService;
import org.fedai.eggroll.clustermanager.dao.impl.ProcessorResourceService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


@Singleton
public class ResourceStateHandler implements StateHandler<ErProcessor> {

    @Inject
    ProcessorResourceService processorResourceService;

    @Inject
    NodeResourceService nodeResourceService;

    @Inject
    ClusterResourceManager clusterResourceManager;

    private static Cache<Long, ReentrantLock> nodeResourceLockCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
            .build();

    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        if (data.getResources() == null || data.getResources().size() == 0) {
            List<ErResource> resourcesList = new ArrayList<>();
            ProcessorResource processorResource = new ProcessorResource();
            processorResource.setProcessorId(data.getId());
            List<ProcessorResource> list = processorResourceService.list(processorResource);
            list.forEach((v) -> resourcesList.add(v.toErResource()));
            data.setResources(resourcesList);
        }
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

    @Override
    public void asynPostHandle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        this.clusterResourceManager.countAndUpdateNodeResource(data.getServerNodeId());
    }

    public void preAllocateResource(ErProcessor erProcessor) {
        LockUtils.lock(nodeResourceLockCache, erProcessor.getServerNodeId());
        try {
            nodeResourceService.preAllocateResource(erProcessor);
            processorResourceService.preAllocateResource(erProcessor);
        } finally {
            LockUtils.unLock(nodeResourceLockCache, erProcessor.getServerNodeId());
        }
    }

    public void preAllocateFailedResource(ErProcessor erProcessor) {
        LockUtils.lock(nodeResourceLockCache, erProcessor.getServerNodeId());
        try {
            nodeResourceService.preAllocateFailed(erProcessor);
            processorResourceService.preAllocateFailed(erProcessor);
        } finally {
            LockUtils.unLock(nodeResourceLockCache, erProcessor.getServerNodeId());
        }
    }


    public void allocatedResource(ErProcessor erProcessor) {
        LockUtils.lock(nodeResourceLockCache, erProcessor.getServerNodeId());
        try {
            nodeResourceService.allocatedResource(erProcessor);
            processorResourceService.allocatedResource(erProcessor);
        } finally {
            LockUtils.unLock(nodeResourceLockCache, erProcessor.getServerNodeId());
        }
    }

    public void returnResource(ErProcessor erProcessor) {
        LockUtils.lock(nodeResourceLockCache, erProcessor.getServerNodeId());
        try {
            nodeResourceService.returnResource(erProcessor);
            processorResourceService.returnResource(erProcessor);
        } finally {
            LockUtils.unLock(nodeResourceLockCache, erProcessor.getServerNodeId());
        }
    }


}