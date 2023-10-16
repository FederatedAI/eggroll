package com.webank.eggroll.webapp.service;


import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.webapp.entity.NodeInfo;

import java.util.ArrayList;
import java.util.List;

public class NodeSituationService {

    @Inject
    private ServerNodeService serverNodeService;
    @Inject
    private NodeResourceService nodeResourceService;


    public List<NodeInfo> getNodeDetails() {


        List<NodeInfo> nodeInfos = new ArrayList<>();
        // 查询 server_node 表中的所有数据
        List<ServerNode> serverNodes = serverNodeService.list();
        // 查询 node_resource 表中的所有数据
        List<NodeResource> nodeResources = nodeResourceService.list();
        // 将 nodeDetails 和 nodeResources 中的数据合并到 nodeDetails 中
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
