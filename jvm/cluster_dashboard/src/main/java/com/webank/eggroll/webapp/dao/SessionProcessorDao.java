package com.webank.eggroll.webapp.dao;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SessionProcessorDao {

    Logger logger = LoggerFactory.getLogger(SessionProcessorDao.class);

    @Inject
    SessionProcessorService sessionProcessorService;

    public PageInfo<SessionProcessor> queryData(SessionProcessorQO sessionProcessorQO) {
        PageHelper.startPage(sessionProcessorQO.getPageNum(), sessionProcessorQO.getPageSize(), true);
        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        queryWrapper.orderByDesc("created_at");
        if (StringUtils.isNotBlank(sessionProcessorQO.getSessionId())
                || StringUtils.isNotBlank(sessionProcessorQO.getStatus())
                || StringUtils.isNotBlank(sessionProcessorQO.getCreatedAt())
                || sessionProcessorQO.getPid() != null) {
            queryWrapper.lambda()
                    .like(StringUtils.isNotBlank(sessionProcessorQO.getSessionId()), SessionProcessor::getSessionId, sessionProcessorQO.getSessionId())
                    .and(StringUtils.isNotBlank(sessionProcessorQO.getStatus()), i -> i.like(SessionProcessor::getStatus, sessionProcessorQO.getStatus()))
                    .and(StringUtils.isNotBlank(sessionProcessorQO.getCreatedAt()), i -> i.like(SessionProcessor::getCreatedAt, sessionProcessorQO.getCreatedAt()))
                    .and(sessionProcessorQO.getPid() != null, i -> i.like(SessionProcessor::getPid, sessionProcessorQO.getPid()));
        }
        List<SessionProcessor> list = this.sessionProcessorService.list(queryWrapper);
        PageInfo<SessionProcessor> result = new PageInfo<>(list);
        return result;
    }


}
