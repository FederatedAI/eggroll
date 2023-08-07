package com.webank.eggroll.clustermanager.statemechine;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.constant.ResourceType;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErResource;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.clustermanager.entity.SessionOption;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public   class ResourceStateHandler implements  StateHandler<ErProcessor>{

        @Autowired
        ServerNodeService  serverNodeService;
        @Autowired
        ProcessorResourceService  processorResourceService;
        @Autowired
        NodeResourceService  nodeResourceService;



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
            return  data;
        }

    private void updateResource(ErProcessor  erProcessor,String desState){
            this.processorResourceService.update(new LambdaUpdateWrapper<ProcessorResource>().set(ProcessorResource::getStatus,desState)
                    .eq(ProcessorResource::getProcessorId,erProcessor.getId()));
    }

    private void preAllocateResource(ErProcessor erProcessor) {
            this.processorResourceService.insertProcessorResource(erProcessor);

    }



}