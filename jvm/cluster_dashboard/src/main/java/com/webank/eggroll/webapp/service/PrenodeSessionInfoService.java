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
        List<String> sessionIds = new ArrayList<>();
        for (SessionProcessor sessionProcessor : sessionProcessors) {
            sessionIds.add(sessionProcessor.getSessionId());
        }
        QueryWrapper sessionMainWrapper = new QueryWrapper();
//        sessionIds 判空
        sessionMainWrapper.in("session_id",sessionIds);
        List<SessionMain> list = sessionMainService.list(sessionMainWrapper);
        return list;
    }

    public List<SessionMain> getNodeSessions(String sessionId){
        // todo 改成直接用sessionid查询的方式
        List<String> sessionIdList = new ArrayList<>();
        sessionIdList.add(sessionId);

        List<SessionMain> list = sessionMainService.listByIds(sessionIdList);
        return list;
    }

}
