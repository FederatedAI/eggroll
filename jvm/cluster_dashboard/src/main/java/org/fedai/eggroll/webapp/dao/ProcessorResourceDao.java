package org.fedai.eggroll.webapp.dao;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.webapp.dao.impl.ProcessorResourceService;
import org.fedai.eggroll.clustermanager.entity.ProcessorResource;
import org.fedai.eggroll.webapp.queryobject.ProcessorResourceQO;
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
        queryWrapper.orderByDesc("created_at");
        // 保存非空检查结果到变量中
        boolean hasProcessorId = StringUtils.isNotBlank(processorResourceQO.getProcessorId());
        boolean hasSessionId = StringUtils.isNotBlank(processorResourceQO.getSessionId());
        boolean hasServerNodeId = StringUtils.isNotBlank(processorResourceQO.getServerNodeId());
        boolean hasStatus = StringUtils.isNotBlank(processorResourceQO.getStatus());
        boolean hasResourceType = StringUtils.isNotBlank(processorResourceQO.getResourceType());

        // 构建查询条件
        if (hasProcessorId || hasSessionId || hasServerNodeId || hasStatus || hasResourceType) {
            queryWrapper.lambda()
                    .like(hasProcessorId, ProcessorResource::getProcessorId, processorResourceQO.getProcessorId())
                    .and(hasSessionId, i -> i.like(ProcessorResource::getSessionId, processorResourceQO.getSessionId()))
                    .and(hasServerNodeId, i -> i.like(ProcessorResource::getServerNodeId, processorResourceQO.getServerNodeId()))
                    .and(hasStatus, i -> i.like(ProcessorResource::getStatus, processorResourceQO.getStatus()))
                    .and(hasResourceType, i -> i.like(ProcessorResource::getResourceType, processorResourceQO.getResourceType()));
        }
        List<ProcessorResource> list = this.processorResourceService.list(queryWrapper);
        PageInfo<ProcessorResource> result = new PageInfo<>(list);
        return result;
    }
}
