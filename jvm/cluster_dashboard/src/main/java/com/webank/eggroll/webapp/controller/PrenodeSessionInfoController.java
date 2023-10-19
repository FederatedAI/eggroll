package com.webank.eggroll.webapp.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.NodeDetailQO;
import com.webank.eggroll.webapp.service.PrenodeSessionInfoService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;
import com.webank.eggroll.webapp.utils.RequestUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@Singleton
public class PrenodeSessionInfoController extends HttpServlet {

    private PrenodeSessionInfoService prenodeSessionInfoService;

    @Inject
    public PrenodeSessionInfoController(PrenodeSessionInfoService prenodeSessionInfoService) {
        this.prenodeSessionInfoService = prenodeSessionInfoService;
    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeDetailQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), NodeDetailQO.class);

        ResponseResult<List<SessionMain>> response;
        Integer nodeNum = nodeDetailQO.getNodeNum();
        String sessionId = nodeDetailQO.getSessionId();
        boolean isSessionId = (sessionId != null && !sessionId.isEmpty());
        if (nodeNum > 0 ) {
            List<SessionMain> resources = prenodeSessionInfoService.getNodeSessions(nodeNum);
            response = RequestUtils.isResourceExist(resources);
        } else if (isSessionId) {
            List<SessionMain> resources = prenodeSessionInfoService.getNodeSessions(sessionId);
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
