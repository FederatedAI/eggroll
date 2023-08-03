package com.webank.eggroll.clustermanager.dao.impl;

import com.eggroll.core.pojo.ErNodeHeartbeat;
import com.eggroll.core.pojo.ErResource;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Service
public class ClusterManagerService {

    Logger log = LoggerFactory.getLogger(ClusterManagerService.class);

    @Autowired
    ServerNodeService serverNodeService;

    @Autowired
    NodeResourceService nodeResourceService;

    Map<Long, ErNodeHeartbeat> nodeHeartbeatMap = new ConcurrentHashMap<>();

    public ErNodeHeartbeat nodeHeartbeat(ErNodeHeartbeat nodeHeartbeat) {
        ErServerNode serverNode = nodeHeartbeat.getNode();
        synchronized (serverNode.getId().toString().intern()) {
            if (serverNode.getId() == -1) {
                ServerNode existNode = serverNodeService.getByEndPoint(serverNode.getEndpoint());
                if (existNode == null) {
                    log.info("create new node {}", JsonUtil.object2Json(serverNode));
                    serverNodeService.createByErNode(serverNode);
                } else {
                    log.info("node already exist {}", existNode);
                    serverNode.setId(existNode.getServerNodeId());
                    updateNode(serverNode, true, true);
                }
            } else {
                if (nodeHeartbeatMap.containsKey(serverNode.getId())
                        && (nodeHeartbeatMap.get(serverNode.getId()).getId() < nodeHeartbeat.getId())) {
                    //正常心跳
                    updateNode(serverNode, false, true);
                } else {
                    //nodemanger重启过
                    ServerNode existsServerNode = serverNodeService.getById(serverNode.getId());
                    if (existsServerNode == null) {
                        serverNode = createNewNode(serverNode);
                    } else {
                        updateNode(serverNode, true, true);
                    }
                }
            }
            nodeHeartbeatMap.put(serverNode.getId(), nodeHeartbeat);
            nodeHeartbeat.setNode(serverNode);
        }
        return nodeHeartbeat;
    }

    public ErServerNode updateNode(ErServerNode serverNode, Boolean needUpdateResource, Boolean isHeartbeat) {
        serverNodeService.updateByErNode(serverNode, isHeartbeat);
        if (needUpdateResource) {
            registerResource(serverNode);
        }
        return serverNode;
    }

    public ErServerNode registerResource(ErServerNode data) {
        log.info("node {} register resource {}", data.getId(), JsonUtil.object2Json(data.getResources()));
        NodeResource nodeResource = new NodeResource();
        nodeResource.setServerNodeId(data.getId());
        List<NodeResource> nodeResourceList = nodeResourceService.list(nodeResource);
        List<ErResource> existResources = new ArrayList<>();
        for (NodeResource resource : nodeResourceList) {
            ErResource erResource = new ErResource();
            BeanUtils.copyProperties(resource, erResource);
            existResources.add(erResource);
        }
        List<ErResource> registedResources = data.getResources();
        List<ErResource> updateResources = new ArrayList<>();
        List<ErResource> deleteResources = new ArrayList<>();
        List<ErResource> insertResources = new ArrayList<>();
        for (ErResource e : existResources) {
            boolean needUpdate = false;
            for (ErResource r : registedResources) {
                if (r.getResourceType().equals(e.getResourceType())) {
                    ErResource updatedResource = new ErResource();
                    BeanUtils.copyProperties(r, updatedResource);
                    updatedResource.setAllocated(-1);
                    needUpdate = true;
                    updateResources.add(updatedResource);
                }
            }
            if (!needUpdate) {
                deleteResources.add(e);
            }
        }

        for (ErResource r : registedResources) {
            if (!updateResources.contains(r)) {
                insertResources.add(r);
            }
        }

        nodeResourceService.registerResource(data.getId(), insertResources, updateResources, deleteResources);
        return data;
    }

    public ErServerNode createNewNode(ErServerNode serverNode) {
        ServerNode existNode = serverNodeService.createByErNode(serverNode);
        serverNode.setId(existNode.getServerNodeId());
        return registerResource(serverNode);
    }
}
