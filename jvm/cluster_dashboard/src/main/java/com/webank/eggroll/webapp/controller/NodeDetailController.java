package com.webank.eggroll.webapp.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.webapp.entity.NodeDetail;
import com.webank.eggroll.webapp.entity.NodeInfo;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.NodeDetailQO;
import com.webank.eggroll.webapp.service.NodeDetailService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;
import com.webank.eggroll.webapp.utils.RequestUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class NodeDetailController extends HttpServlet {
    //获取节点机器详情

    private NodeDetailService nodeDetailService;

    @Inject
    public NodeDetailController(NodeDetailService nodeDetailService) {
        this.nodeDetailService = nodeDetailService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeDetailQO nodeDetailQO = objectMapper.readValue(req.getInputStream(), NodeDetailQO.class);
        int nodeNum = nodeDetailQO.getNodeNum();
        String sessionId = nodeDetailQO.getSessionId();
        boolean isSessionId = (sessionId != null && !sessionId.isEmpty());
        if (nodeNum > 0) {// 获取单个机器详情
            writeResponse(resp, getDetailByNodeNum(nodeNum));
        } else if (isSessionId) { // 获取session下的机器详情
            writeResponse(resp, getNodeBySessionId(sessionId));
        } else {
            writeResponse(resp, new ResponseResult(ErrorCode.PARAM_ERROR));
        }

    }
    private void writeResponse(HttpServletResponse resp, ResponseResult<?> response) throws IOException {
        if (response != null) {
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            String json = JsonFormatUtil.toJson(response.getCode(), response.getMsg(), response.getData());
            resp.getWriter().write(json);
        } else {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Response is null");
        }
    }

    private ResponseResult<Map<String, NodeDetail>> getDetailByNodeNum(int nodeNum) {
        ResponseResult<Map<String, NodeDetail>> response;
        Map<String, NodeDetail> resources = new HashMap<>();
        if (nodeNum > 0) {
            resources = nodeDetailService.getNodeDetails(nodeNum);
            if (resources != null && !resources.isEmpty()) {
                // 获取数据成功
                response = ResponseResult.success(resources);
            } else {
                // 获取数据失败或无数据
                response = new ResponseResult(ErrorCode.DATA_ERROR);
            }
        } else {
            response = new ResponseResult(ErrorCode.PARAM_ERROR);
        }
        return response;
    }

    private ResponseResult<List<NodeInfo>> getNodeBySessionId(String sessionId) {
        ResponseResult<List<NodeInfo>> response;
        List<NodeInfo> resources = nodeDetailService.getNodeDetails(sessionId);
        if (resources != null && !resources.isEmpty()) {
            // 获取数据成功
            response = ResponseResult.success(resources);
        } else {
            // 获取数据失败或无数据
            response = new ResponseResult(ErrorCode.DATA_ERROR);
        }
        return response;
    }

}