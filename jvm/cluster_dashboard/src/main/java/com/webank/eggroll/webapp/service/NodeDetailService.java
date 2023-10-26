package com.webank.eggroll.webapp.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
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
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.NodeDetailQO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeDetailService {

    Logger logger = LoggerFactory.getLogger(NodeDetailService.class);


    @Inject
    private ServerNodeService serverNodeService;

    @Inject
    private NodeResourceService nodeResourceService;

    @Inject
    private SessionMainService sessionMainService;

    @Inject
    private SessionProcessorService sessionProcessorService;

    public Object queryNodeDetail(NodeDetailQO nodeDetailQO) {
        Integer nodeNum = nodeDetailQO.getNodeNum();
        String sessionId = nodeDetailQO.getSessionId();
        PageHelper.startPage(nodeDetailQO.getPageNum(), nodeDetailQO.getPageSize());
        boolean isSessionId = (sessionId != null && !sessionId.isEmpty());
        if (nodeNum > 0) {// 获取单个机器详情
            return getNodeDetails(nodeNum);
        } else if (isSessionId) { // 获取session下的机器详情
            return getNodeDetails(sessionId);
        }
        return new ResponseResult(ErrorCode.PARAM_ERROR);
    }

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
        QueryWrapper serverNodeWrapper = new QueryWrapper();
        QueryWrapper nodeResourceWrapper = new QueryWrapper();
        if (distinctServerNodeIds != null && !distinctServerNodeIds.isEmpty()) {
            serverNodeWrapper.in("server_node_id", distinctServerNodeIds);
            nodeResourceWrapper.in("server_node_id", distinctServerNodeIds);
        } else {
            // 如果列表为空，添加一个不成立的条件，例如 ID 为负数的情况
            serverNodeWrapper.in("server_node_id", -1);
            nodeResourceWrapper.in("server_node_id", -1);
        }
        // todo 如果查询出来的数据为空，应判空处理
        List<ServerNode> serverNodes = serverNodeService.list(serverNodeWrapper);
        // 用根据distinctServerNodeIds查询node_resource表中的数据
        List<NodeResource> nodeResources = nodeResourceService.list(nodeResourceWrapper);

        List<NodeInfo> nodeInfos = new ArrayList<>();
        if ((serverNodes == null || serverNodes.isEmpty()) || (nodeResources == null || nodeResources.isEmpty())) {
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
                    nodeInfo.setCreatedAt(serverNode.getCreatedAt());
                    nodeInfo.setUpdatedAt(serverNode.getUpdatedAt());
                    nodeInfos.add(nodeInfo);
                }
            }
        }
        return nodeInfos;
    }

}
