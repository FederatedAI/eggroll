package org.fedai.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.entity.ServerNode;

import org.fedai.eggroll.webapp.queryobject.ServerNodeQO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ServerNodeDao {

    Logger logger = LoggerFactory.getLogger(ServerNodeDao.class);

    @Inject
    ServerNodeService serverNodeService;

    public PageInfo<ServerNode> queryData(ServerNodeQO serverNodeQO) {
        PageHelper.startPage(serverNodeQO.getPageNum(), serverNodeQO.getPageSize(),true);

        QueryWrapper<ServerNode> queryWrapper = new QueryWrapper<>();
        queryWrapper.orderByDesc("created_at");
        boolean hasServerNodeId = StringUtils.isNotBlank(serverNodeQO.getServerNodeId());
        boolean hasName = StringUtils.isNotBlank(serverNodeQO.getName());
        boolean hasServerClusterId = StringUtils.isNotBlank(serverNodeQO.getServerClusterId());
        boolean hasNodeType = StringUtils.isNotBlank(serverNodeQO.getNodeType());
        boolean hasStatus = StringUtils.isNotBlank(serverNodeQO.getStatus());
        boolean hasLastHeartbeatAt = StringUtils.isNotBlank(serverNodeQO.getLastHeartbeatAt());
        // 构建查询条件
        if (hasServerNodeId || hasName || hasServerClusterId || hasNodeType || hasStatus || hasLastHeartbeatAt) {
            queryWrapper.lambda()
                    .like(hasServerNodeId, ServerNode::getServerNodeId, serverNodeQO.getServerNodeId())
                    .and(hasName, i -> i.like(ServerNode::getName, serverNodeQO.getName()))
                    .and(hasServerClusterId, i -> i.like(ServerNode::getServerClusterId, serverNodeQO.getServerClusterId()))
                    .and(hasNodeType, i -> i.like(ServerNode::getNodeType, serverNodeQO.getNodeType()))
                    .and(hasStatus, i -> i.like(ServerNode::getStatus, serverNodeQO.getStatus()))
                    .and(hasLastHeartbeatAt, i -> i.like(ServerNode::getLastHeartbeatAt, serverNodeQO.getLastHeartbeatAt()));
        }
        List<ServerNode> list = this.serverNodeService.list(queryWrapper);
        PageInfo<ServerNode> result = new PageInfo<>(list);
        return result;
    }

}
