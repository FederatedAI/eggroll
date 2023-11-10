package com.webank.eggroll.webapp.dao;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.webapp.queryobject.SessionMainQO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SessionMainDao {

    Logger logger = LoggerFactory.getLogger(SessionMainDao.class);

    @Inject
    SessionMainService sessionMainService;


    public Object topQueryOrQueryData(SessionMainQO sessionMainQO) {
        // 根据topCount参数是否有值，判断该查询前top数据还是查询所有数据
        Integer topCount = sessionMainQO.getTopCount();
        if (topCount != null && topCount > 0) {
            return topQuery(sessionMainQO,topCount);
        } else {
            return queryData(sessionMainQO);
        }
    }


    public PageInfo<SessionMain> queryData(SessionMainQO sessionMainQO) {// 查询所有数据，包含分页和模糊查询
        PageHelper.startPage(sessionMainQO.getPageNum(), sessionMainQO.getPageSize(),true);
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        // 添加默认创建时间倒序排序
        queryWrapper.orderByDesc("created_at");
        boolean hasSessionId = StringUtils.isNotBlank(sessionMainQO.getSessionId());
        boolean hasName = StringUtils.isNotBlank(sessionMainQO.getName());
        boolean createdAt = (sessionMainQO.getCreatedAt() != null);
        boolean hasStatus = StringUtils.isNotBlank(sessionMainQO.getStatus());
        // 构建查询条件
        if (hasSessionId || hasName || createdAt || hasStatus) {
            queryWrapper.lambda()
                    .like(hasSessionId, SessionMain::getSessionId, sessionMainQO.getSessionId())
                    .and(hasName, i -> i.like(SessionMain::getName, sessionMainQO.getName()))
                    .and(createdAt, i -> i.like(SessionMain::getCreatedAt, sessionMainQO.getCreatedAt()))
                    .and(hasStatus, i -> i.like(SessionMain::getStatus, sessionMainQO.getStatus()));

        }
        List<SessionMain> list = this.sessionMainService.list(queryWrapper);
        PageInfo<SessionMain> result = new PageInfo<>(list);
        return result;
    }

    public PageInfo<SessionMain> topQuery(SessionMainQO sessionMainQO,int topCount) {
        PageHelper.startPage(sessionMainQO.getPageNum(), sessionMainQO.getPageSize(),true);
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("status", "NEW", "ACTIVATED", "CLOSED", "KILLED")  // 状态为NEW或ACTIVATED
                .orderByDesc("created_at")
                // 按照 updated_at 降序排序
                .last("LIMIT " + topCount);          // 取前 topCount 条数据");
        List<SessionMain> list = this.sessionMainService.list(queryWrapper);
        PageInfo<SessionMain> result = new PageInfo<>(list);
        return result;
    }

    public Long queryActiveSession(){
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("status", "ACTIVATED" );
        return this.sessionMainService.count(queryWrapper);
    }

    public Long queryNewSession(){
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("status", "NEW" );
        return this.sessionMainService.count(queryWrapper);
    }
}
