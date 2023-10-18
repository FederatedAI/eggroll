package com.webank.eggroll.webapp.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

import java.util.ArrayList;
import java.util.List;

public class PrenodeSessionInfoService {

//    @Inject
//    private ServerNodeService serverNodeService;

    @Inject
    private SessionProcessorService sessionProcessorService;

    @Inject
    private SessionMainService sessionMainService;

    public List<SessionMain> getNodeSessions(int nodeNum){

        QueryWrapper spWrapper = new QueryWrapper();
        spWrapper.eq("server_node_id", nodeNum);
        List<SessionProcessor> sessionProcessors = sessionProcessorService.list(spWrapper);
        List<String> sessionIs = new ArrayList<>();
        for (SessionProcessor sessionProcessor : sessionProcessors) {
            sessionIs.add(sessionProcessor.getSessionId());
        }
        QueryWrapper sessionMainWrapper = new QueryWrapper();
        sessionMainWrapper.in("session_id",sessionIs);
        List<SessionMain> list = sessionMainService.list(sessionMainWrapper);
        return list;
    }

    public List<SessionMain> getNodeSessions(String sessionId){

        List<String> sessionIdList = new ArrayList<>();
        sessionIdList.add(sessionId);
        List<SessionMain> list = sessionMainService.listByIds(sessionIdList);
        return list;
    }

}
