package com.webank.eggroll.clustermanager.statemechine;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.constant.ResourceType;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErResource;
import com.eggroll.core.pojo.ErServerNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
@Service
public class NodeResourceCounter implements  Callback{

    @Autowired
    ProcessorResourceService processorResourceService;

    @Autowired
    NodeResourceService nodeResourceService;

    protected   boolean  checkNeedChangeResource(Context context){
        return  context.isNeedDispatchResource();
    }


    public void   countAndUpdateNodeResource(Long serverNodeId){



        List<ProcessorResource> resourceList = this.processorResourceService.list( new LambdaQueryWrapper<ProcessorResource>()
                .eq(ProcessorResource::getServerNodeId, serverNodeId).in(ProcessorResource::getResourceType, Lists.newArrayList(ResourceStatus.ALLOCATED.getValue() ,ResourceStatus.PRE_ALLOCATED.getValue())));
        List<ErResource>  prepareUpdateResource = Lists.newArrayList();
        if(resourceList!=null){
            Map<String,ErResource> resourceMap = Maps.newHashMap();
            resourceList.forEach(processorResource->{
                String status = processorResource.getStatus();
                ErResource nodeResource = resourceMap.get(processorResource.getResourceType());
                if(nodeResource==null){
                    resourceMap.put(processorResource.getResourceType(),new ErResource());
                    nodeResource = resourceMap.get(processorResource.getResourceType());
                }
                if(status.equals(ResourceStatus.ALLOCATED.getValue())){
                    if(nodeResource.getAllocated()!=null)
                        nodeResource.setAllocated(nodeResource.getAllocated()+processorResource.getAllocated());
                    else
                        nodeResource.setAllocated(processorResource.getAllocated());

                }else{
                    if(nodeResource.getPreAllocated()!=null)
                        nodeResource.setPreAllocated(nodeResource.getPreAllocated()+processorResource.getAllocated());
                    else
                        nodeResource.setPreAllocated(processorResource.getAllocated());
                }
            });
            prepareUpdateResource.addAll(resourceMap.values());
        }else{
            ErResource gpuResource = new ErResource();
            gpuResource.setServerNodeId(serverNodeId);
            gpuResource.setResourceType(ResourceType.VGPU_CORE.name());
            gpuResource.setAllocated(0L);
            gpuResource.setPreAllocated(0L);
            ErResource  cpuResource = new ErResource();
            cpuResource.setServerNodeId(serverNodeId);
            cpuResource.setResourceType(ResourceType.VCPU_CORE.name());
            cpuResource.setAllocated(0L);
            cpuResource.setPreAllocated(0L);
            prepareUpdateResource.add(gpuResource);
            prepareUpdateResource.add(cpuResource);
        }
        nodeResourceService.doUpdateNodeResource(serverNodeId,prepareUpdateResource);
    }


    @Override
    public void callback(Context context, Object o) {
        if(checkNeedChangeResource(context)) {
            List<ErServerNode> serverNodes = (List<ErServerNode>) context.getData(Dict.SERVER_NODES);
            if (serverNodes != null) {
                for (ErServerNode erServerNode : serverNodes) {
                    countAndUpdateNodeResource(erServerNode.getId());
                }
            }
        }
    }
}
