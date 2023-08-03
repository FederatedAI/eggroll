package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.pojo.ErResource;
import com.webank.eggroll.clustermanager.dao.mapper.NodeResourceMapper;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class NodeResourceService extends EggRollBaseServiceImpl<NodeResourceMapper, NodeResource>{

    Logger log = LoggerFactory.getLogger(NodeResourceService.class);
    @Transactional
    public void registerResource(Long serverNodeId , List<ErResource> insertData,List<ErResource> updateData,List<ErResource> deleteData){
        if(insertData!=null&&insertData.size()>0){
            doInsertNodeResource(serverNodeId,insertData);
        }

        if(updateData!=null&&updateData.size()>0){
            doUpdateNodeResource(serverNodeId,updateData);
        }
        if(deleteData!=null&&deleteData.size()>0){
            doDeleteNodeResource(serverNodeId,deleteData);
        }
    }

    public void doInsertNodeResource(Long serverNodeId ,@NotNull List<ErResource> resources){
        log.info("insertNodeResource======== {} ,size {}",serverNodeId,resources.size());
        for (ErResource erResource : resources) {
            try {
                NodeResource nodeResource = new NodeResource();
                nodeResource.setServerNodeId(serverNodeId);
                nodeResource.setResourceType(erResource.getResourceType());
                nodeResource.setTotal(erResource.getTotal());
                nodeResource.setUsed(erResource.getUsed());
                nodeResource.setStatus(erResource.getStatus());
                this.save(nodeResource);
            } catch (Exception e) {
                log.error("doInsertNodeResource Exception : {}" + e.getMessage());
            }
        }
    }

    public void doUpdateNodeResource(Long serverNodeId ,@NotNull List<ErResource> resources){
        for (ErResource erResource : resources) {
            UpdateWrapper<NodeResource> updateWrapper = new UpdateWrapper<>();
            updateWrapper.lambda().set(erResource.getTotal() >= 0,NodeResource::getTotal,erResource.getTotal())
                    .set(erResource.getAllocated()>= 0,NodeResource::getAllocated,erResource.getAllocated())
                    .set(erResource.getPreAllocated()>=0,NodeResource::getPreAllocated,erResource.getPreAllocated())
                    .set(erResource.getUsed() >= 0,NodeResource::getUsed,erResource.getUsed())
                    .set(erResource.getExtention()!=null,NodeResource::getExtention,erResource.getExtention())
                    .eq(StringUtils.isNotBlank(erResource.getResourceType()),NodeResource::getResourceType,erResource.getResourceType())
                    .eq(serverNodeId >= 0,NodeResource::getServerNodeId,serverNodeId);
            this.update(updateWrapper);
        }
    }

    public void doDeleteNodeResource(Long serverNodeId ,@NotNull List<ErResource> resources){
        for (ErResource erResource : resources) {
            QueryWrapper<NodeResource> deleteWrapper = new QueryWrapper<>();
            deleteWrapper.lambda().eq(NodeResource::getServerNodeId,serverNodeId)
                    .eq(NodeResource::getResourceType,erResource.getResourceType());
            this.remove(deleteWrapper);
        }
    }


}
