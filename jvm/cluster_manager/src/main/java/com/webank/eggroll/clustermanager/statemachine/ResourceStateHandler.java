package com.webank.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Singleton
public   class ResourceStateHandler implements  StateHandler<ErProcessor>{

        @Autowired
        @Inject
        ServerNodeService  serverNodeService;
        @Autowired
        @Inject
        ProcessorResourceService  processorResourceService;
        @Autowired
        @Inject
        NodeResourceService  nodeResourceService;

        @Autowired
        @Inject
        ClusterResourceManager    clusterResourceManager;

        @Override
        public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
            return data;
        }

        @Override
        public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
            String stateLine =  preStateParam+"_"+desStateParam;
            switch (stateLine){
                case "init_pre_allocated":
                    preAllocateResource(data);
                    break;
                case "pre_allocated_allocated": ;
                case "pre_allocated_allocate_failed":;
                case "allocated_return":
                    updateResource(data,desStateParam);
                    break;
                }
            this.openAsynPostHandle(context);
            return  data;
        }

    private void updateResource(ErProcessor  erProcessor,String desState){
            this.processorResourceService.update(new LambdaUpdateWrapper<ProcessorResource>().set(ProcessorResource::getStatus,desState)
                    .eq(ProcessorResource::getProcessorId,erProcessor.getId()));
    }

    private void preAllocateResource(ErProcessor erProcessor) {
            this.processorResourceService.insertProcessorResource(erProcessor);
    }


      public void asynPostHandle(Context context, ErProcessor data, String preStateParam, String desStateParam){
            this.clusterResourceManager.countAndUpdateNodeResource(data.getServerNodeId());
      };




}