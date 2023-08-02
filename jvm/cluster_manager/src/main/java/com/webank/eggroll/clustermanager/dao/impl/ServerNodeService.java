package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErServerNode;
import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ServerNodeService extends EggRollBaseServiceImpl<ServerNodeMapper, ServerNode> {

    public List<ErServerNode> doGetServerNodes(ErServerNode input){
        QueryWrapper<ServerNode> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda()
                .eq(input.getId()>0,ServerNode::getServerNodeId,input.getId())
                .eq(!StringUtils.isBlank(input.getName()),ServerNode::getName,input.getName())
                .eq(input.getClusterId() >= 0,ServerNode::getServerClusterId,input.getClusterId())
                .eq(!StringUtils.isBlank(input.getEndpoint().getHost()),ServerNode::getHost,input.getEndpoint().getHost())
                .eq(input.getEndpoint().getPort()>0,ServerNode::getPort,input.getEndpoint().getPort())
                .eq(!StringUtils.isBlank(input.getNodeType()),ServerNode::getNodeType,input.getNodeType())
                .eq(!StringUtils.isBlank(input.getStatus()),ServerNode::getStatus,input.getStatus())
                .orderByAsc(ServerNode::getServerNodeId);
        List<ServerNode> ServerNodeList = list(queryWrapper);
        List<ErServerNode> result = new ArrayList<>();
        for (ServerNode serverNode : ServerNodeList) {
            ErServerNode erServerNode = new ErServerNode();

            erServerNode.setId(serverNode.getServerNodeId());
            erServerNode.setName(serverNode.getName());
            erServerNode.setClusterId(serverNode.getServerClusterId());

            ErEndpoint erEndpoint = new ErEndpoint(serverNode.getHost(), serverNode.getPort());

            erServerNode.setEndpoint(erEndpoint);
            erServerNode.setNodeType(serverNode.getNodeType());
            erServerNode.setStatus(serverNode.getStatus());
//            erServerNode.setLastHeartBeat(serverNode.getLastHeartbeatAt());
            result.add(erServerNode);
        }
        return result;
    }

}
