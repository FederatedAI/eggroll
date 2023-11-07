package com.webank.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapp.queryobject.ProcessorResourceQO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcessorResourceDao {

    Logger logger = LoggerFactory.getLogger(ProcessorResourceDao.class);

    @Inject
    ProcessorResourceService processorResourceService;

    public Object query(ProcessorResourceQO processorResourceQO) {


        return processorResourceService.list();
    }

    public Object queryData(ProcessorResourceQO processorResourceQO) {
        PageHelper.startPage(processorResourceQO.getPageNum(), processorResourceQO.getPageSize(),true);

        QueryWrapper<ProcessorResource> queryWrapper = new QueryWrapper<>();
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
        List<ProcessorResource> list = this.processorResourceService.list(queryWrapper);
        PageInfo<ProcessorResource> result = new PageInfo<>(list);
        return result;
    }
}
