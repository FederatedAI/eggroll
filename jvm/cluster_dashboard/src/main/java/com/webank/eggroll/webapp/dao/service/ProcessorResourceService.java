package com.webank.eggroll.webapp.dao.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErResource;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.EggRollBaseServiceImpl;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.clustermanager.resource.ResourceManager;
import com.webank.eggroll.webapp.dao.mapper.ProcessorResourceMapper;

@Singleton
public class ProcessorResourceService extends EggRollBaseServiceImpl<ProcessorResourceMapper, ProcessorResource> implements ResourceManager {

    @Override
    public void preAllocateResource(ErProcessor erProcessor) {
        for (ErResource erResource : erProcessor.getResources()) {
            ProcessorResource processorResource = new ProcessorResource();
            processorResource.setProcessorId(erProcessor.getId());
            processorResource.setResourceType(erResource.getResourceType());
            processorResource.setAllocated(erResource.getAllocated());
            processorResource.setExtention(erResource.getExtention());
            processorResource.setStatus(erResource.getStatus());
            processorResource.setSessionId(erProcessor.getSessionId());
            processorResource.setServerNodeId(erProcessor.getServerNodeId().intValue());
            this.save(processorResource);
        }
    }

    @Override
    public void preAllocateFailed(ErProcessor erProcessor) {
        updateResource(erProcessor, "return");
    }

    @Override
    public void allocatedResource(ErProcessor erProcessor) {
        updateResource(erProcessor, "allocated");
    }

    @Override
    public void returnResource(ErProcessor erProcessor) {
        updateResource(erProcessor, "return");
    }

    public void updateResource(ErProcessor erProcessor, String desState) {
        for (ErResource erResource : erProcessor.getResources()) {
            ProcessorResource processorResource = new ProcessorResource();
            processorResource.setStatus(desState);
            LambdaQueryWrapper<ProcessorResource> updateWrapper = new LambdaQueryWrapper<>();
            updateWrapper.eq(ProcessorResource::getProcessorId, erResource.getProcessorId())
                    .eq(ProcessorResource::getSessionId, erResource.getSessionId())
                    .eq(ProcessorResource::getServerNodeId, erResource.getServerNodeId())
                    .eq(ProcessorResource::getResourceType, erResource.getResourceType());
            this.update(processorResource, updateWrapper);
        }
    }
}