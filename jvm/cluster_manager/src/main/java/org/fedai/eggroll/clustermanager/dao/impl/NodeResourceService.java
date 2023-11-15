package org.fedai.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.clustermanager.dao.mapper.NodeResourceMapper;
import org.fedai.eggroll.clustermanager.entity.NodeResource;
import org.fedai.eggroll.clustermanager.resource.ResourceManager;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.fedai.eggroll.core.pojo.ErResource;
import org.mybatis.guice.transactional.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Singleton
public class NodeResourceService extends EggRollBaseServiceImpl<NodeResourceMapper, NodeResource> implements ResourceManager {

    Logger log = LoggerFactory.getLogger(NodeResourceService.class);

    @Transactional
    public void registerResource(Long serverNodeId, List<ErResource> insertData, List<ErResource> updateData, List<ErResource> deleteData) {
        if (insertData != null && insertData.size() > 0) {
            doInsertNodeResource(serverNodeId, insertData);
        }
        if (updateData != null && updateData.size() > 0) {
            doUpdateNodeResource(serverNodeId, updateData);
        }
        if (deleteData != null && deleteData.size() > 0) {
            doDeleteNodeResource(serverNodeId, deleteData);
        }
    }

    public void doInsertNodeResource(Long serverNodeId, List<ErResource> resources) {
        log.info("insertNodeResource======== {} ,size {}", serverNodeId, resources.size());
        List<NodeResource> nodeResourceList = Lists.newArrayList();
        for (ErResource erResource : resources) {
            NodeResource nodeResource = new NodeResource();
            nodeResource.setServerNodeId(serverNodeId);
            nodeResource.setResourceType(erResource.getResourceType());
            nodeResource.setTotal(erResource.getTotal());
            nodeResource.setUsed(erResource.getUsed());
            nodeResource.setStatus(erResource.getStatus());
            this.save(nodeResource);
        }
    }

    public void doUpdateNodeResource(Long serverNodeId, List<ErResource> resources) {
        for (ErResource erResource : resources) {
            UpdateWrapper<NodeResource> updateWrapper = new UpdateWrapper<>();
            updateWrapper.lambda().set(erResource.getTotal() >= 0, NodeResource::getTotal, erResource.getTotal())
                    .set(erResource.getAllocated() >= 0, NodeResource::getAllocated, erResource.getAllocated())
                    .set(erResource.getPreAllocated() >= 0, NodeResource::getPreAllocated, erResource.getPreAllocated())
                    .set(erResource.getUsed() >= 0, NodeResource::getUsed, erResource.getUsed())
                    .set(erResource.getExtention() != null, NodeResource::getExtention, erResource.getExtention())
                    .eq(StringUtils.isNotBlank(erResource.getResourceType()), NodeResource::getResourceType, erResource.getResourceType())
                    .eq(serverNodeId >= 0, NodeResource::getServerNodeId, serverNodeId);
            this.update(updateWrapper);
        }
    }

    public void doDeleteNodeResource(Long serverNodeId, List<ErResource> resources) {
        for (ErResource erResource : resources) {
            QueryWrapper<NodeResource> deleteWrapper = new QueryWrapper<>();
            deleteWrapper.lambda().eq(NodeResource::getServerNodeId, serverNodeId)
                    .eq(NodeResource::getResourceType, erResource.getResourceType());
            this.remove(deleteWrapper);
        }
    }

    @Override
    public void preAllocateResource(ErProcessor erProcessor) {
        final List<ErResource> processorResources = erProcessor.getResources();
        if (processorResources == null || processorResources.size() == 0) {
            return;
        }
        Map<String, NodeResource> resourceMap = new HashMap<>();
        for (ErResource resource : processorResources) {
            NodeResource nodeResource = resourceMap.get(resource.getResourceType());
            if (nodeResource == null) {
                nodeResource = new NodeResource();
                nodeResource.setServerNodeId(erProcessor.getServerNodeId());
                nodeResource.setResourceType(resource.getResourceType());
                nodeResource = this.get(nodeResource);
                if (nodeResource == null) {
                    throw new RuntimeException("nodeResource serverNodeId = " + erProcessor.getServerNodeId() + ", resourceType = " + resource.getResourceType() + " missing");
                }
                resourceMap.put(resource.getResourceType(), nodeResource);
            }
            nodeResource.setPreAllocated(nodeResource.getPreAllocated() + resource.getAllocated());
            if (StringUtils.isBlank(nodeResource.getExtention())) {
                nodeResource.setExtention(resource.getExtention());
            } else {
                List<String> extensionList = new ArrayList<>(Arrays.asList(nodeResource.getExtention().split(",")));
                boolean exists = extensionList.stream().anyMatch((extension) -> extension.equals(resource.getExtention()));
                if (!exists) {
                    extensionList.add(resource.getExtention());
                }
                nodeResource.setExtention(String.join(",", extensionList));
            }
            resourceMap.forEach((k, v) -> this.updateById(v));
        }
    }

    @Override
    public void preAllocateFailed(ErProcessor erProcessor) {
        final List<ErResource> processorResources = erProcessor.getResources();
        if (processorResources == null || processorResources.size() == 0) {
            return;
        }
        Map<String, NodeResource> resourceMap = new HashMap<>();
        for (ErResource resource : processorResources) {
            NodeResource nodeResource = resourceMap.get(resource.getResourceType());
            if (nodeResource == null) {
                nodeResource = new NodeResource();
                nodeResource.setServerNodeId(erProcessor.getServerNodeId());
                nodeResource.setResourceType(resource.getResourceType());
                nodeResource = this.get(nodeResource);
                if (nodeResource == null) {
                    throw new RuntimeException("nodeResource serverNodeId = " + erProcessor.getServerNodeId() + ", resourceType = " + resource.getResourceType() + " missing");
                }
                resourceMap.put(resource.getResourceType(), nodeResource);
            }
            nodeResource.setPreAllocated(nodeResource.getPreAllocated() - resource.getAllocated());
            if (StringUtils.isNotBlank(nodeResource.getExtention())) {
                List<String> extensionList = new ArrayList<>(Arrays.asList(nodeResource.getExtention().split(",")));
                extensionList.removeIf((extension) -> extension.equals(resource.getExtention()));
                nodeResource.setExtention(String.join(",", extensionList));
            }
            resourceMap.forEach((k, v) -> this.updateById(v));
        }
    }

    @Override
    public void allocatedResource(ErProcessor erProcessor) {
        final List<ErResource> processorResources = erProcessor.getResources();
        if (processorResources == null || processorResources.size() == 0) {
            return;
        }
        Map<String, NodeResource> resourceMap = new HashMap<>();
        for (ErResource resource : processorResources) {
            NodeResource nodeResource = resourceMap.get(resource.getResourceType());
            if (nodeResource == null) {
                nodeResource = new NodeResource();
                nodeResource.setServerNodeId(erProcessor.getServerNodeId());
                nodeResource.setResourceType(resource.getResourceType());
                nodeResource = this.get(nodeResource);
                if (nodeResource == null) {
                    throw new RuntimeException("nodeResource serverNodeId = " + erProcessor.getServerNodeId() + ", resourceType = " + resource.getResourceType() + " missing");
                }
                resourceMap.put(resource.getResourceType(), nodeResource);
            }
            nodeResource.setPreAllocated(nodeResource.getPreAllocated() - resource.getAllocated());
            nodeResource.setAllocated(nodeResource.getAllocated() + resource.getAllocated());
            nodeResource.setUsed(nodeResource.getUsed() + resource.getAllocated());
            resourceMap.forEach((k, v) -> this.updateById(v));
        }
    }

    @Override
    public void returnResource(ErProcessor erProcessor) {
        final List<ErResource> processorResources = erProcessor.getResources();
        if (processorResources == null || processorResources.size() == 0) {
            return;
        }
        Map<String, NodeResource> resourceMap = new HashMap<>();
        for (ErResource resource : processorResources) {
            NodeResource nodeResource = resourceMap.get(resource.getResourceType());
            if (nodeResource == null) {
                nodeResource = new NodeResource();
                nodeResource.setServerNodeId(erProcessor.getServerNodeId());
                nodeResource.setResourceType(resource.getResourceType());
                nodeResource = this.get(nodeResource);
                if (nodeResource == null) {
                    throw new RuntimeException("nodeResource serverNodeId = " + erProcessor.getServerNodeId() + ", resourceType = " + resource.getResourceType() + " missing");
                }
                resourceMap.put(resource.getResourceType(), nodeResource);
            }
            if (StringUtils.isNotBlank(nodeResource.getExtention())) {
                List<String> extensionList = new ArrayList<>(Arrays.asList(nodeResource.getExtention().split(",")));
                extensionList.removeIf((extension) -> extension.equals(resource.getExtention()));
                nodeResource.setExtention(String.join(",", extensionList));
            }
            nodeResource.setAllocated(nodeResource.getAllocated() - resource.getAllocated());
            nodeResource.setUsed(nodeResource.getUsed() - resource.getAllocated());
            resourceMap.forEach((k, v) -> this.updateById(v));
        }
    }


}
