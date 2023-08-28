package com.webank.eggroll.clustermanager.dao.impl;

import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErResource;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.mapper.ProcessorResourceMapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;


import java.util.List;

@Singleton
public class ProcessorResourceService extends EggRollBaseServiceImpl<ProcessorResourceMapper, ProcessorResource>{

    public  void  insertProcessorResource(ErProcessor  erProcessor){
        List<ProcessorResource>  insertResources = Lists.newArrayList();
        for(ErResource erResource: erProcessor.getResources()){
            ProcessorResource  processorResource = new  ProcessorResource();
            processorResource.setProcessorId(erProcessor.getId());
            processorResource.setResourceType(erResource.getResourceType());
            processorResource.setAllocated(erResource.getAllocated());
            processorResource.setExtention(erResource.getExtention());
            processorResource.setStatus(erResource.getStatus());
            processorResource.setSessionId(erProcessor.getSessionId());
            processorResource.setServerNodeId(erProcessor.getServerNodeId().intValue());
            insertResources.add(processorResource);
        }
        this.saveBatch(insertResources);
    }

}
