package com.webank.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.webapp.queryobject.NodeResourceQO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
public class NodeResourceDao {

    Logger logger = LoggerFactory.getLogger(NodeResourceDao.class);

    @Inject
    NodeResourceService nodeResourceService;

    public List<NodeResource> queryData(NodeResourceQO nodeResourceQO) {
        PageHelper.startPage(nodeResourceQO.getPageNum(), nodeResourceQO.getPageSize());
        QueryWrapper<NodeResource> queryWrapper = new QueryWrapper<>();

        if (StringUtils.isNotBlank(nodeResourceQO.getResourceId())
                || StringUtils.isNotBlank(nodeResourceQO.getServerNodeId())
                || StringUtils.isNotBlank(nodeResourceQO.getStatus())
                || StringUtils.isNotBlank(nodeResourceQO.getResourceType())) {
            queryWrapper.and(wrapper ->
                    wrapper.like(StringUtils.isNotBlank(nodeResourceQO.getResourceId()), "resource_id", nodeResourceQO.getResourceId())
                            .or()
                            .like(StringUtils.isNotBlank(nodeResourceQO.getServerNodeId()), "server_node_id", nodeResourceQO.getServerNodeId())
                            .or()
                            .like(StringUtils.isNotBlank(nodeResourceQO.getStatus()), "status", nodeResourceQO.getStatus())
                            .or()
                            .like(StringUtils.isNotBlank(nodeResourceQO.getResourceType()), "resource_type", nodeResourceQO.getResourceType())
            );
        }

        return this.nodeResourceService.list(queryWrapper);
    }
}
