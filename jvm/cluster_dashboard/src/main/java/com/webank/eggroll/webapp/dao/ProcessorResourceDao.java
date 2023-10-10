package com.webank.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapp.queryobject.ProcessorResourceQO;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class ProcessorResourceDao {
    @Inject
    ProcessorResourceService processorResourceService;

    public List<ProcessorResource> queryData(ProcessorResourceQO processorResourceQO) {
        PageHelper.startPage(processorResourceQO.getPageNum(), processorResourceQO.getPageSize());

        QueryWrapper<ProcessorResource> queryWrapper = new QueryWrapper<>();

//        if (StringUtils.isNotBlank(processorResourceQO.getProcessorId())
//                || StringUtils.isNotBlank(processorResourceQO.getSessionId())
//                || StringUtils.isNotBlank(processorResourceQO.getServerNodeId())
//                || StringUtils.isNotBlank(processorResourceQO.getStatus())
//                || StringUtils.isNotBlank(processorResourceQO.getResourceType())) {
//            queryWrapper.and(wrapper ->
//                    wrapper.like(StringUtils.isNotBlank(processorResourceQO.getProcessorId()), "processor_id", processorResourceQO.getProcessorId())
//                            .or()
//                            .like(StringUtils.isNotBlank(processorResourceQO.getServerNodeId()), "server_node_id", processorResourceQO.getServerNodeId())
//                            .or()
//                            .like(StringUtils.isNotBlank(processorResourceQO.getStatus()), "status", processorResourceQO.getStatus())
//                            .or()
//                            .like(StringUtils.isNotBlank(processorResourceQO.getResourceType()), "resource_type", processorResourceQO.getResourceType())
//                            .or()
//                            .like(StringUtils.isNotBlank(processorResourceQO.getSessionId()), "session_id", processorResourceQO.getSessionId())
//            );
//        }
        // 保存非空检查结果到变量中
        boolean hasProcessorId = StringUtils.isNotBlank(processorResourceQO.getProcessorId());
        boolean hasSessionId = StringUtils.isNotBlank(processorResourceQO.getSessionId());
        boolean hasServerNodeId = StringUtils.isNotBlank(processorResourceQO.getServerNodeId());
        boolean hasStatus = StringUtils.isNotBlank(processorResourceQO.getStatus());
        boolean hasResourceType = StringUtils.isNotBlank(processorResourceQO.getResourceType());

        // 构建查询条件
        if (hasProcessorId || hasSessionId || hasServerNodeId || hasStatus || hasResourceType) {
            queryWrapper.and(wrapper -> {
                if (hasProcessorId) wrapper.like("processor_id", processorResourceQO.getProcessorId());
                if (hasServerNodeId) wrapper.or().like("server_node_id", processorResourceQO.getServerNodeId());
                if (hasStatus) wrapper.or().like("status", processorResourceQO.getStatus());
                if (hasResourceType) wrapper.or().like("resource_type", processorResourceQO.getResourceType());
                if (hasSessionId) wrapper.or().like("session_id", processorResourceQO.getSessionId());
            });
        }


        return this.processorResourceService.list(queryWrapper);
    }
}
