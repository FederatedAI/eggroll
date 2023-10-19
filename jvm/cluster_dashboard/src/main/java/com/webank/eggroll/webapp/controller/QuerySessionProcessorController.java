package com.webank.eggroll.webapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import com.webank.eggroll.webapp.service.QuerySessionProcessorService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;
import com.webank.eggroll.webapp.utils.RequestUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;


@Singleton
public class QuerySessionProcessorController extends HttpServlet {
    private QuerySessionProcessorService querySessionService;

    @Inject
    public QuerySessionProcessorController(QuerySessionProcessorService querySessionService) {
        this.querySessionService = querySessionService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);

        Integer serverNodeId = sessionProcessorQO.getServerNodeId();
        String sessionId = sessionProcessorQO.getSessionId();
        int pageNum = sessionProcessorQO.getPageNum();
        int pageSize = sessionProcessorQO.getPageSize();

        ResponseResult<List<SessionProcessor>> response;
        if (serverNodeId > 0 && (sessionId != null && !sessionId.isEmpty())) {
            List<SessionProcessor> resources = querySessionService.getSessionProcessors(serverNodeId, sessionId, pageNum, pageSize);
            response = RequestUtils.isResourceExist(resources);
        } else {
            response = new ResponseResult(ErrorCode.PARAM_ERROR);
        }
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        // 将响应结果转换为 JSON 格式
        String json = JsonFormatUtil.toJson(response.getCode(),
                response.getMsg(), response.getData());

        resp.getWriter().write(json);
    }
}
