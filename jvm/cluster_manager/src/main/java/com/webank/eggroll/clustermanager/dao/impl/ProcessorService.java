package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErResource;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;


import java.util.List;
import java.util.stream.Collectors;


@Singleton
public class ProcessorService extends EggRollBaseServiceImpl<SessionProcessorMapper,SessionProcessor>{


    @Inject
    ProcessorResourceService   processorResourceService ;


    public List<ErProcessor> getProcessorBySession(String sessionId,boolean withResource){
        List<ErProcessor>  processors = this.list(new LambdaQueryWrapper<SessionProcessor>().eq(SessionProcessor::getSessionId,sessionId))
                 .stream().map((x)->{ return x.toErProcessor();}).collect(Collectors.toList());
          if(withResource){
              processors.forEach(erProcessor -> {
                  List<ProcessorResource>  resourceList =  processorResourceService.list(new LambdaQueryWrapper<ProcessorResource>().eq(ProcessorResource::getProcessorId,erProcessor.getId()));
                  List<ErResource> changedList  = resourceList.stream().map(processorResource -> {  return processorResource.toErResource();}).collect(Collectors.toList());
                  erProcessor.setResources(changedList);
              });
          }
          return processors;
    }









}
