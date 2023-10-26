package com.webank.eggroll.webapp.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;

import java.util.List;

public class QuerySessionProcessorService {

    @Inject
    private SessionProcessorService sessionProcessorService;

    public Object query(SessionProcessorQO sessionProcessorQO) {
        Integer serverNodeId = sessionProcessorQO.getServerNodeId();
        String sessionId = sessionProcessorQO.getSessionId();
        int pageNum = sessionProcessorQO.getPageNum();
        int pageSize = sessionProcessorQO.getPageSize();

        if (serverNodeId > 0 && (sessionId != null && !sessionId.isEmpty())) {
            List<SessionProcessor> resources = getSessionProcessors(serverNodeId, sessionId, pageNum, pageSize);
            return resources;
        }else {
            return new ResponseResult(ErrorCode.PARAM_ERROR);
        }
    }

    public List<SessionProcessor> getSessionProcessors(int serverNodeId, String sessionId, Integer pageNum, Integer pageSize) {
        PageHelper.startPage(pageNum, pageSize);

        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("server_node_id", serverNodeId);
        queryWrapper.eq("session_id", sessionId);
        return sessionProcessorService.list(queryWrapper);
    }
}
