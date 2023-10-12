package com.webank.eggroll.webapp.service;


import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.webapp.entity.NodeDetail;

import java.util.List;

public class NodeSituationService {

    @Inject
    private ServerNodeService serverNodeService;
    @Inject
    private NodeResourceService nodeResourceService;


    public List<NodeDetail> getNodeDetails() {

        NodeDetail nodeDetail = new NodeDetail();
        List<NodeDetail> nodeDetails = null;
        // 查询 server_node 表中的所有数据
        List<ServerNode> serverNodes = serverNodeService.list();
        // 查询 node_resource 表中的所有数据
        List<NodeResource> nodeResources = nodeResourceService.list();
        // 将 nodeDetails 和 nodeResources 中的数据合并到 nodeDetails 中
        for (ServerNode serverNode : serverNodes) {
            for (NodeResource nodeResource : nodeResources) {
                if (serverNode.getServerNodeId().equals(nodeResource.getServerNodeId())) {
                    nodeDetail.setResourceId(nodeResource.getResourceId());
                    nodeDetail.setServerNodeId(nodeResource.getServerNodeId());
                    nodeDetail.setResourceType(nodeResource.getResourceType());
                    nodeDetail.setTotal(nodeResource.getTotal());
                    nodeDetail.setUsed(nodeResource.getUsed());
                    nodeDetail.setPreAllocated(nodeResource.getPreAllocated());
                    nodeDetail.setAllocated(nodeResource.getAllocated());
                    nodeDetail.setExtention(nodeResource.getExtention());
                    nodeDetail.setServerStatus(serverNode.getStatus());
                    nodeDetail.setResourceStatus(nodeResource.getStatus());
                    nodeDetail.setName(serverNode.getName());
                    nodeDetail.setServerClusterId(serverNode.getServerClusterId());
                    nodeDetail.setHost(serverNode.getHost());
                    nodeDetail.setPort(serverNode.getPort());
                    nodeDetail.setNodeType(serverNode.getNodeType());
                    nodeDetail.setLastHeartbeatAt(serverNode.getLastHeartbeatAt());
                    nodeDetails.add(nodeDetail);
                }
            }
        }

        return nodeDetails;
    }

}
