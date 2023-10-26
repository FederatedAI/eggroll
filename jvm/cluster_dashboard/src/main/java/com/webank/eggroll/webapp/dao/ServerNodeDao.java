package com.webank.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.webapp.queryobject.ServerNodeQO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ServerNodeDao {

    Logger logger = LoggerFactory.getLogger(ServerNodeDao.class);

    @Inject
    ServerNodeService serverNodeService;

    public List<ServerNode> queryData(ServerNodeQO serverNodeQO) {
        PageHelper.startPage(serverNodeQO.getPageNum(), serverNodeQO.getPageSize());

        QueryWrapper<ServerNode> queryWrapper = new QueryWrapper<>();
        boolean hasServerNodeId = StringUtils.isNotBlank(serverNodeQO.getServerNodeId());
        boolean hasName = StringUtils.isNotBlank(serverNodeQO.getName());
        boolean hasServerClusterId = StringUtils.isNotBlank(serverNodeQO.getServerClusterId());
        boolean hasNodeType = StringUtils.isNotBlank(serverNodeQO.getNodeType());
        boolean hasStatus = StringUtils.isNotBlank(serverNodeQO.getStatus());
        boolean hasLastHeartbeatAt = StringUtils.isNotBlank(serverNodeQO.getLastHeartbeatAt());
        // 构建查询条件
        if (hasServerNodeId || hasName || hasServerClusterId || hasNodeType || hasStatus || hasLastHeartbeatAt) {
            queryWrapper.and(wrapper -> {
                if (hasServerNodeId) wrapper.like("server_node_id", serverNodeQO.getServerNodeId());
                if (hasName) wrapper.or().like("name", serverNodeQO.getName());
                if (hasServerClusterId) wrapper.or().like("server_cluster_id", serverNodeQO.getServerClusterId());
                if (hasNodeType) wrapper.or().like("node_type", serverNodeQO.getNodeType());
                if (hasStatus) wrapper.or().like("status", serverNodeQO.getStatus());
                if (hasLastHeartbeatAt) wrapper.or().like("last_heartbeat_at", serverNodeQO.getLastHeartbeatAt());
            });
        }

        return this.serverNodeService.list(queryWrapper);
    }

}
