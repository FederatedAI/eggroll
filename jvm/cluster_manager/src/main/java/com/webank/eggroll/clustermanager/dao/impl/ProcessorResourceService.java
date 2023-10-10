package com.webank.eggroll.clustermanager.dao.impl;

import com.eggroll.core.config.Dict;
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
//        if(erProcessor.getResources().size() == 0 ){
//            ErResource GPU_Resource = new ErResource();
//            GPU_Resource.setServerNodeId(erProcessor.getServerNodeId());
//            GPU_Resource.setResourceType(Dict.VGPU_CORE);
//            GPU_Resource.setPreAllocated(0L);
//            GPU_Resource.setAllocated(0L);
//            GPU_Resource.setExtention("");
//            erProcessor.getResources().add(GPU_Resource);
//
//            ErResource CPU_Resource = new ErResource();
//            CPU_Resource.setServerNodeId(erProcessor.getServerNodeId());
//            CPU_Resource.setResourceType(Dict.VCPU_CORE);
//            CPU_Resource.setPreAllocated(0L);
//            CPU_Resource.setAllocated(0L);
//            CPU_Resource.setExtention("");
//            erProcessor.getResources().add(CPU_Resource);
//        }
        for(ErResource erResource: erProcessor.getResources()){
            ProcessorResource  processorResource = new  ProcessorResource();
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

}
