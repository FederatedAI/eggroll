package com.webank.eggroll.webapp.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.entity.NodeDetail;
import com.webank.eggroll.webapp.entity.NodeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeDetailService {

    @Inject
    private ServerNodeService serverNodeService;

    @Inject
    private NodeResourceService nodeResourceService;

    @Inject
    private SessionMainService sessionMainService;

    @Inject
    private SessionProcessorService sessionProcessorService;


    public Map<String, NodeDetail> getNodeDetails(int nodeNum) {
        //根据节点id获取节点详情 nodeNum为节点id
        Map<String, NodeDetail> nodeInfos = new HashMap<>();
        // 根据 nodeNum 查询 server_node 表中的数据
        ServerNode serverNode = serverNodeService.getById(nodeNum);
        // 根据 nodeNum 查询 node_resource 表中的数据
        QueryWrapper queryWrapper = new QueryWrapper();
        queryWrapper.eq("server_node_id", nodeNum);
        List<NodeResource> nodeResources = nodeResourceService.list(queryWrapper);
        // 创建map集合，map的key是node_resource表中的resource_type字段。
        for (NodeResource nodeResource : nodeResources) {
            if (serverNode.getServerNodeId().equals(nodeResource.getServerNodeId())) {
                NodeDetail nodeInfo = new NodeDetail();
                nodeInfo.setResourceId(nodeResource.getResourceId());
                nodeInfo.setServerNodeId(nodeResource.getServerNodeId());
                nodeInfo.setResourceType(nodeResource.getResourceType());
                nodeInfo.setTotal(nodeResource.getTotal());
                nodeInfo.setUsed(nodeResource.getUsed());
                nodeInfo.setPreAllocated(nodeResource.getPreAllocated());
                nodeInfo.setAllocated(nodeResource.getAllocated());
                nodeInfo.setExtention(nodeResource.getExtention());
                nodeInfo.setStatus(nodeResource.getStatus());
                nodeInfo.setCreatedAt(nodeResource.getCreatedAt());
                nodeInfo.setUpdatedAt(nodeResource.getUpdatedAt());

                nodeInfos.put(nodeInfo.getResourceType(), nodeInfo);
            }
        }
        return nodeInfos;
    }

    public List<NodeInfo> getNodeDetails(String sessionId) {
        QueryWrapper queryWrapper = new QueryWrapper();
        queryWrapper.eq("session_id", sessionId);
        List<SessionProcessor> sessionProcessors = sessionProcessorService.list(queryWrapper);

        List<Integer> distinctServerNodeIds = new ArrayList<>();
        for (SessionProcessor sessionProcessor : sessionProcessors) {
            Integer serverNodeId = sessionProcessor.getServerNodeId();
            if (!distinctServerNodeIds.contains(serverNodeId)) {
                distinctServerNodeIds.add(serverNodeId);
            }
        }
        // 用根据distinctServerNodeIds查询server_node表中的数据
        List<ServerNode> serverNodes = serverNodeService.listByIds(distinctServerNodeIds);
        // 用根据distinctServerNodeIds查询node_resource表中的数据
        QueryWrapper nodeResourceWrapper = new QueryWrapper();
        nodeResourceWrapper.in("server_node_id", distinctServerNodeIds);
        List<NodeResource> nodeResources = nodeResourceService.list(nodeResourceWrapper);

        List<NodeInfo> nodeInfos = new ArrayList<>();
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
                    nodeInfo.setCreatedAt(serverNode.getCreatedAt());
                    nodeInfo.setUpdatedAt(serverNode.getUpdatedAt());
                    nodeInfos.add(nodeInfo);
                }
            }
        }
        return nodeInfos;
    }

}