package com.webank.eggroll.webapp.dao;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.webapp.queryobject.SessionMainQO;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class SessionMainDao {
    @Inject
    SessionMainService sessionMainService;
    public List<SessionMain> queryData(SessionMainQO sessionMainQO) {
        PageHelper.startPage(sessionMainQO.getPageNum(), sessionMainQO.getPageSize());
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();

        boolean hasSessionId = StringUtils.isNotBlank(sessionMainQO.getSessionId());
        boolean hasName = StringUtils.isNotBlank(sessionMainQO.getName());
        boolean hasTag = StringUtils.isNotBlank(sessionMainQO.getTag());
        boolean hasStatus = StringUtils.isNotBlank(sessionMainQO.getStatus());
        // 构建查询条件
        if (hasSessionId || hasName || hasTag || hasStatus) {
            queryWrapper.and(wrapper -> {
                if (hasSessionId) wrapper.like("session_id", sessionMainQO.getSessionId());
                if (hasName) wrapper.or().like("name", sessionMainQO.getName());
                if (hasTag) wrapper.or().like("tag", sessionMainQO.getTag());
                if (hasStatus) wrapper.or().like("status", sessionMainQO.getStatus());
            });
        }
        return this.sessionMainService.list(queryWrapper);
    }
}
