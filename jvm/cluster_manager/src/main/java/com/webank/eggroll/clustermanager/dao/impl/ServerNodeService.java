package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErServerNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.apache.commons.lang3.StringUtils;

import org.mybatis.guice.transactional.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class ServerNodeService extends EggRollBaseServiceImpl<ServerNodeMapper, ServerNode> {
    Logger logger = LoggerFactory.getLogger(ServerNodeService.class);
    LoadingCache<Long,ErServerNode> cache ;
    ServerNodeService(){
        cache=  CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(300, TimeUnit.SECONDS)
                .build(new CacheLoader<Long,ErServerNode>(){
                    @Override
                    public ErServerNode load(Long serverNodeId) throws Exception {
                        logger.info("node server miss cache, try to load from db , id {}",serverNodeId);
                        ServerNode  serverNode = getById(serverNodeId);
                        if(serverNode!=null){
                            return  serverNode.toErServerNode();
                        }else{
                            return null;
                        }
                    }
                });
    }
    public ErServerNode getByIdFromCache(Long serverNodeId){
        ErServerNode  result= null;
        try {
            result = this.cache.get(serverNodeId);
        } catch (Exception e) {

        }
        if(result==null)
            logger.info("server node cache {}",cache.asMap());
        return  result;
    }


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
        logger.info("create new node {}",input);
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
