package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErServerNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.apache.commons.lang3.StringUtils;

import org.mybatis.guice.transactional.Transactional;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class ServerNodeService extends EggRollBaseServiceImpl<ServerNodeMapper, ServerNode> {

    @Inject
    NodeResourceService nodeResourceService;

    public ServerNode getByEndPoint( ErEndpoint input) {
        ServerNode serverNode = new ServerNode();
        serverNode.setHost(input.getHost());
        serverNode.setPort(input.getPort());
        List<ServerNode> nodeList = this.list(serverNode);
        return nodeList.size() > 0 ? nodeList.get(0) : null;
    }

    @Transactional
    public ServerNode createByErNode(ErServerNode input) {
        ServerNode serverNode = new ServerNode();
        serverNode.setServerNodeId(input.getId() > 0 ? input.getId() : null);
        serverNode.setName(input.getName());
        serverNode.setServerClusterId(input.getClusterId());
        serverNode.setHost(input.getEndpoint().getHost());
        serverNode.setPort(input.getEndpoint().getPort());
        serverNode.setNodeType(input.getNodeType());
        serverNode.setStatus(input.getStatus());
        this.save(serverNode);
        return serverNode;
    }

    @Transactional
    public void updateByErNode(ErServerNode input, Boolean isHeartbeat) {
        ServerNode serverNode = new ServerNode();
        serverNode.setServerNodeId(input.getId());
        serverNode.setName(input.getName());
        serverNode.setHost(input.getEndpoint().getHost());
        serverNode.setPort(input.getEndpoint().getPort());
        serverNode.setStatus(input.getStatus());
        if (isHeartbeat) {
            serverNode.setLastHeartbeatAt(new Date());
        }
        this.updateById(serverNode);
    }

    public List<ErServerNode> getListByErServerNode(ErServerNode input) {
        QueryWrapper<ServerNode> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda()
                .eq(input.getId() > 0, ServerNode::getServerNodeId, input.getId())
                .eq(!StringUtils.isBlank(input.getName()), ServerNode::getName, input.getName())
                .eq(input.getClusterId() >= 0, ServerNode::getServerClusterId, input.getClusterId())
                .eq(!StringUtils.isBlank(input.getEndpoint().getHost()), ServerNode::getHost, input.getEndpoint().getHost())
                .eq(input.getEndpoint().getPort() > 0, ServerNode::getPort, input.getEndpoint().getPort())
                .eq(!StringUtils.isBlank(input.getNodeType()), ServerNode::getNodeType, input.getNodeType())
                .eq(!StringUtils.isBlank(input.getStatus()), ServerNode::getStatus, input.getStatus())
                .orderByAsc(ServerNode::getServerNodeId);
        List<ServerNode> ServerNodeList = list(queryWrapper);
        List<ErServerNode> result = new ArrayList<>();
        for (ServerNode serverNode : ServerNodeList) {
            result.add(serverNode.toErServerNode());
        }
        return result;
    }

    public List<ErServerNode> getServerNodesWithResource(ErServerNode input){
        List<ErServerNode> serverNodes = getListByErServerNode(input);
        for (ErServerNode serverNode : serverNodes) {
            List<NodeResource> nodeResources = nodeResourceService.list(new QueryWrapper<NodeResource>().lambda().eq(NodeResource::getServerNodeId, serverNode.getId()));
            if(nodeResources != null){
                serverNode.setResources(nodeResources.stream().map(NodeResource::toErResource).collect(Collectors.toList()));
            }
        }
        return serverNodes;
    }

}
