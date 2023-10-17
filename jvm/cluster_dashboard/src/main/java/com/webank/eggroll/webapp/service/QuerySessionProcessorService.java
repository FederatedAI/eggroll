package com.webank.eggroll.webapp.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

import java.util.List;

public class QuerySessionProcessorService {

    @Inject
    private SessionProcessorService sessionProcessorService;

    public List<SessionProcessor> getSessionProcessors(int serverNodeId, String sessionId, Integer pageNum, Integer pageSize) {
        PageHelper.startPage(pageNum, pageSize);

        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("server_node_id", serverNodeId);
        queryWrapper.eq("session_id", sessionId);
        return sessionProcessorService.list(queryWrapper);
    }
}
