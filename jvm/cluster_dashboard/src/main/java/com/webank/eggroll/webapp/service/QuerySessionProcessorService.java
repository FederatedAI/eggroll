package com.webank.eggroll.webapp.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QuerySessionProcessorService {

    Logger logger = LoggerFactory.getLogger(QuerySessionProcessorService.class);

    @Inject
    private SessionProcessorService sessionProcessorService;

    public Object query(SessionProcessorQO sessionProcessorQO) {
        Integer serverNodeId = sessionProcessorQO.getServerNodeId();
        String sessionId = sessionProcessorQO.getSessionId();
        int pageNum = sessionProcessorQO.getPageNum();
        int pageSize = sessionProcessorQO.getPageSize();

        if (serverNodeId > 0 && (sessionId != null && !sessionId.isEmpty())) {
            return getSessionProcessors(serverNodeId, sessionId, pageNum, pageSize);
        } else {
            return new ResponseResult(ErrorCode.PARAM_ERROR);
        }
    }

    public PageInfo<SessionProcessor> getSessionProcessors(int serverNodeId, String sessionId, Integer pageNum, Integer pageSize) {
        PageHelper.startPage(pageNum, pageSize,true);

        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("server_node_id", serverNodeId);
        queryWrapper.eq("session_id", sessionId);
        List<SessionProcessor> list = sessionProcessorService.list(queryWrapper);
        PageInfo<SessionProcessor> result = new PageInfo<>(list);
        return result;
    }
}
