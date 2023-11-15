package com.webank.eggroll.webapp.controller;

import org.fedai.eggroll.core.context.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.QueueViewService;
import com.webank.eggroll.webapp.dao.*;
import com.webank.eggroll.webapp.dao.service.*;
import com.webank.eggroll.webapp.interfaces.ApiMethod;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;

@Singleton
public class EggrollServiceProvider {

    Logger logger = LoggerFactory.getLogger(EggrollServiceProvider.class);

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

    @Inject
    private NodeResourceDao nodeResourceDao;

    @Inject
    private SessionProcessorDao sessionProcessorDao;

    @Inject
    private SessionMainDao sessionMainDao;

    @Inject
    private ContainerStatusService containerStatusService;

    @Inject
    private QueueViewService queueViewService;


    @ApiMethod("/eggroll/nodeSituation")
    public Object getNodeSituation(HttpServletRequest req) {
        return nodeSituationService.getNodeDetails();
    }

    @ApiMethod("/eggroll/nodeDetail")
    public Object getNodeDetail(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeDetailQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), NodeDetailQO.class);
        return nodeDetailService.queryNodeDetail(nodeDetailQO);
    }

    @ApiMethod("/eggroll/querySessionProcessor")
    public Object querySessionProcessor(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);
        return querySessionService.query(sessionProcessorQO);
    }

    @ApiMethod("/eggroll/killSession")
    public Object killSession(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);
        containerStatusService.killSession(sessionProcessorQO);
        return new ResponseResult("kill success");
    }

    // 获取资源分配队列等待请求数接口
    @ApiMethod("/eggroll/getWaitingQueue")
    public Object getWaitingQueue(HttpServletRequest req) throws IOException {
        // 查询等待分配资源的session数量，返回一个Integer类型的数据
        return containerStatusService.getWaitingQueue();
    }

    @ApiMethod("/eggroll/killProcessor")
    public Object killProcessor(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);
        String processorId = sessionProcessorQO.getProcessorId();
        containerStatusService.killProcessor(Long.valueOf(processorId));
        return new ResponseResult("kill success");
    }

    @ApiMethod("/eggroll/queryNodeMetaInfo")
    public Object queryNodeMetaInfo(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ServerNodeQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), ServerNodeQO.class);
        String serverNodeId = nodeDetailQO.getServerNodeId();
        logger.info("queryNodeMetaInfo serverNodeId: {}", serverNodeId);
        Map<String, String> result = containerStatusService.queryNodeMetaInfo(new Context(), Long.valueOf(serverNodeId));
        return new ResponseResult<>(result);
    }


    @ApiMethod("/eggroll/preNodeSessionInfo")
    public Object queryPreNodeSessionInfo(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeDetailQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), NodeDetailQO.class);
        logger.info("queryPreNodeSessionInfo nodeDetailQO: {}", nodeDetailQO);
        return prenodeSessionInfoService.querySession(nodeDetailQO);
    }

    @ApiMethod("/eggroll/processorResource")
    public Object queryProcessorResource(HttpServletRequest req) throws IOException {
        // 查询processor_resource表的所有信息 包含分页和模糊查询
        ObjectMapper objectMapper = new ObjectMapper();
        ProcessorResourceQO processorResourceQO = objectMapper.readValue(req.getInputStream(), ProcessorResourceQO.class);
        logger.info("queryProcessorResource processorResourceQO: {}", processorResourceQO);
        return resourceDao.queryData(processorResourceQO);
    }

    @ApiMethod("/eggroll/serverNode")
    public Object getServerNode(HttpServletRequest req) throws IOException {
        // 查找所有的servernode，可以根据字段进行筛选（模糊查找）
        ObjectMapper objectMapper = new ObjectMapper();
        ServerNodeQO serverNodeQO = objectMapper.readValue(req.getInputStream(), ServerNodeQO.class);
        logger.info("getServerNode serverNodeQO: {}", serverNodeQO);
        return serverNodeDao.queryData(serverNodeQO);
    }

    @ApiMethod("/eggroll/nodeResource")
    public Object getNodeResource(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeResourceQO nodeResourceQO = objectMapper.readValue(req.getInputStream(), NodeResourceQO.class);
        logger.info("getNodeResource nodeResourceQO: {}", nodeResourceQO);
        return nodeResourceDao.queryData(nodeResourceQO);
    }

    @ApiMethod("/eggroll/sessionProcessor")
    public Object getSessionProcessor(HttpServletRequest req) throws IOException {
        // 查询session_processor表的所有信息 包含分页和模糊查询
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);
        logger.info("getSessionProcessor sessionProcessorQO: {}", sessionProcessorQO);
        return sessionProcessorDao.queryData(sessionProcessorQO);
    }

    @ApiMethod("/eggroll/sessionMain")
    public Object getSessionMain(HttpServletRequest req) throws IOException {
        // 这个接口可以根据有没有topCount参数，判断是查询所有数据还是查询前topCount数据，然后调用不同的方法
        // 查询所有数据，包含分页和模糊查询
        ObjectMapper objectMapper = new ObjectMapper();
        SessionMainQO sessionMainQO = objectMapper.readValue(req.getInputStream(), SessionMainQO.class);
        logger.info("getSessionMain sessionMainQO: {}", sessionMainQO);
        return sessionMainDao.topQueryOrQueryData(sessionMainQO);
    }

    @ApiMethod("/eggroll/cpuLineChart")
    public Object getCpuLineChart(HttpServletRequest req) throws IOException {
        // 查询cpu资源的折线图数据，返回一个map类型{节点，剩余cpu数量}
        return nodeResourceDao.queryCpuResources();
    }

    @ApiMethod("/eggroll/gpuLineChart")
    public Object getGpuLineChart(HttpServletRequest req) throws IOException {
        // 查询gpu资源的折线图数据，返回一个map类型{节点，剩余gpu数量}
        return nodeResourceDao.queryGpuResources();
    }

    @ApiMethod("/eggroll/getActiveSession")
    public Object getActiveSession(HttpServletRequest req) throws IOException {
        // 查询活跃的session数量，返回一个long类型的数据
        return sessionMainDao.queryActiveSession();
    }

    @ApiMethod("/eggroll/getNewSession")
    public Object getNewSession(HttpServletRequest req) throws IOException {
        // 查询新建（等待启动）的session数量，返回一个long类型的数据
        return sessionMainDao.queryNewSession();
    }


}
