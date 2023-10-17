package com.webank.eggroll.webapp.dao;


import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class SessionProcessorDao {
    @Inject
    SessionProcessorService sessionProcessorService;

    public List<SessionProcessor> queryData(SessionProcessorQO sessionProcessorQO) {
        PageHelper.startPage(sessionProcessorQO.getPageNum(), sessionProcessorQO.getPageSize());
        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();

        if (StringUtils.isNotBlank(sessionProcessorQO.getSessionId())
                || StringUtils.isNotBlank(sessionProcessorQO.getProcessorId())
                || StringUtils.isNotBlank(sessionProcessorQO.getStatus())
                || StringUtils.isNotBlank(sessionProcessorQO.getCreateTime())
                || StringUtils.isNotBlank(sessionProcessorQO.getUpdateTime())) {
            queryWrapper.and(wrapper ->
                    wrapper.like(StringUtils.isNotBlank(sessionProcessorQO.getSessionId()), "session_id", sessionProcessorQO.getSessionId())
                            .or()
                            .like(StringUtils.isNotBlank(sessionProcessorQO.getProcessorId()), "processor_id", sessionProcessorQO.getProcessorId())
                            .or()
                            .like(StringUtils.isNotBlank(sessionProcessorQO.getStatus()), "status", sessionProcessorQO.getStatus())
                            .or()
                            .like(StringUtils.isNotBlank(sessionProcessorQO.getCreateTime()), "created_at", sessionProcessorQO.getCreateTime())
                            .or()
                            .like(StringUtils.isNotBlank(sessionProcessorQO.getUpdateTime()), "updated_at", sessionProcessorQO.getUpdateTime())
            );
        }

        return this.sessionProcessorService.list(queryWrapper);
    }


}
