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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class NodeResourceDao {

    Logger logger = LoggerFactory.getLogger(NodeResourceDao.class);

    @Inject
    NodeResourceService nodeResourceService;

    public List<NodeResource> queryData(NodeResourceQO nodeResourceQO) {
        PageHelper.startPage(nodeResourceQO.getPageNum(), nodeResourceQO.getPageSize(),true);

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
//        List<NodeResource> list = this.nodeResourceService.list(queryWrapper);

        return this.nodeResourceService.list(queryWrapper);
    }

    // 查询cpu剩余资源数据
    public Map<String,Long> queryCpuResources() {
        QueryWrapper<NodeResource> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("resource_type", "VCPU_CORE");
//        queryWrapper.eq("status", "available");
        List<NodeResource> nodeResourceList = this.nodeResourceService.list(queryWrapper);
        if (nodeResourceList == null || nodeResourceList.size() == 0) {
            return null;
        }
        Long cpuResource = 0L;
        Map<String,Long> resourcesMap  = new HashMap<>();
        for (NodeResource nodeResource : nodeResourceList) {
            String key = String.valueOf(nodeResource.getServerNodeId());
            if (!resourcesMap.containsKey(key)) {
                cpuResource = nodeResource.getTotal() - nodeResource.getUsed();
                resourcesMap.put(key,cpuResource);
            }
        }
        return resourcesMap;
    }

    // 查询GPU剩余资源数据
    public Map<String,Long> queryGpuResources() {
        QueryWrapper<NodeResource> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("resource_type", "VGPU_CORE");
        List<NodeResource> nodeResourceList = this.nodeResourceService.list(queryWrapper);
        if (nodeResourceList == null || nodeResourceList.size() == 0) {
            return null;
        }
        Long gpuResource = 0L;
        Map<String, Long> resourcesMap = new HashMap<>();
        for (NodeResource nodeResource : nodeResourceList) {
            String key = String.valueOf(nodeResource.getServerNodeId());
            if (!resourcesMap.containsKey(key)) {
                gpuResource = nodeResource.getTotal() - nodeResource.getUsed();
                resourcesMap.put(key, gpuResource);
            }
        }
        return resourcesMap;
    }

}
