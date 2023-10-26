package com.webank.eggroll.webapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.PageHelper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.dao.ProcessorResourceDao;
import com.webank.eggroll.webapp.dao.ServerNodeDao;
import com.webank.eggroll.webapp.entity.NodeDetail;
import com.webank.eggroll.webapp.entity.NodeInfo;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.interfaces.ApiMethod;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.NodeDetailQO;
import com.webank.eggroll.webapp.queryobject.ProcessorResourceQO;
import com.webank.eggroll.webapp.queryobject.ServerNodeQO;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import com.webank.eggroll.webapp.service.NodeDetailService;
import com.webank.eggroll.webapp.service.NodeSituationService;
import com.webank.eggroll.webapp.service.PrenodeSessionInfoService;
import com.webank.eggroll.webapp.service.QuerySessionProcessorService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class EggrollServiceProvider {

    private DispatcherServlet dispatcherServlet;

    @Inject
    private NodeSituationService nodeSituationService;
    @Inject
    private NodeDetailService nodeDetailService;

    @Inject
    private QuerySessionProcessorService querySessionService;

    @Inject
    private PrenodeSessionInfoService prenodeSessionInfoService;

    @Inject
    private ProcessorResourceDao resourceDao;

    @Inject
    private ServerNodeDao serverNodeDao;



    @ApiMethod("/api/nodeSituation")
    public Object getNodeSituation(HttpServletRequest req) {
        return nodeSituationService.getNodeDetails();
    }

    @ApiMethod("/api/nodedetail")
    public Object getNodeDetail(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeDetailQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), NodeDetailQO.class);
        return nodeDetailService.queryNodeDetail(nodeDetailQO);
    }

    @ApiMethod("/api/querysessionprocessor")
    public Object querySessionProcessor(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);
        return querySessionService.query(sessionProcessorQO);
    }

    @ApiMethod("/api/prenodesessioninfo")
    public Object queryPreNodeSessionInfo(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeDetailQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), NodeDetailQO.class);

        return prenodeSessionInfoService.querySession(nodeDetailQO);
    }

    @ApiMethod("/api/processorresource")
    public Object queryProcessorResource(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ProcessorResourceQO processorResourceQO = objectMapper.readValue(req.getInputStream(), ProcessorResourceQO.class);
        return resourceDao.queryData(processorResourceQO);
    }

    @ApiMethod("/api/servernode")
    public Object getServerNode(HttpServletRequest req) throws IOException {//还没写完
        ObjectMapper objectMapper = new ObjectMapper();
        ServerNodeQO serverNodeQO = objectMapper.readValue(req.getInputStream(), ServerNodeQO.class);
        return serverNodeDao.queryData(serverNodeQO);
    }

}
