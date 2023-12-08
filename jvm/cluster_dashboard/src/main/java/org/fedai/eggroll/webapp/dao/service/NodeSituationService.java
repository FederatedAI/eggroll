package org.fedai.eggroll.webapp.dao.service;


import com.google.inject.Inject;
import org.fedai.eggroll.webapp.dao.impl.NodeResourceService;
import org.fedai.eggroll.webapp.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.entity.NodeResource;
import org.fedai.eggroll.clustermanager.entity.ServerNode;
import org.fedai.eggroll.webapp.entity.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NodeSituationService{

    Logger logger = LoggerFactory.getLogger(NodeSituationService.class);

    @Inject
    private ServerNodeService serverNodeService;
    @Inject
    private NodeResourceService nodeResourceService;

    public List<NodeInfo> getNodeDetails() {


        List<NodeInfo> nodeInfos = new ArrayList<>();

        List<ServerNode> serverNodes = serverNodeService.list();

        if (serverNodes == null || serverNodes.isEmpty()) {
            return nodeInfos;
        }
        List<NodeResource> nodeResources = nodeResourceService.list();
        if (nodeResources == null || nodeResources.isEmpty()) {
            return nodeInfos;
        }
        for (ServerNode serverNode : serverNodes) {
            for (NodeResource nodeResource : nodeResources) {
                if (serverNode.getServerNodeId().equals(nodeResource.getServerNodeId())) {
                    NodeInfo nodeInfo = new NodeInfo();
                    nodeInfo.setResourceId(nodeResource.getResourceId());
                    nodeInfo.setServerNodeId(nodeResource.getServerNodeId());
                    nodeInfo.setResourceType(nodeResource.getResourceType());
                    nodeInfo.setTotal(nodeResource.getTotal());
                    nodeInfo.setAllocated(nodeResource.getAllocated());
                    nodeInfo.setServerNodeStatus(serverNode.getStatus());
                    nodeInfo.setNodeResourceStatus(nodeResource.getStatus());
                    nodeInfo.setName(serverNode.getName());
                    nodeInfo.setServerClusterId(serverNode.getServerClusterId());
                    nodeInfo.setHost(serverNode.getHost());
                    nodeInfo.setPort(serverNode.getPort());
                    nodeInfo.setNodeType(serverNode.getNodeType());
                    nodeInfo.setLastHeartbeatAt(serverNode.getLastHeartbeatAt());
                    nodeInfos.add(nodeInfo);
                }
            }
        }
        return nodeInfos;
    }

}
