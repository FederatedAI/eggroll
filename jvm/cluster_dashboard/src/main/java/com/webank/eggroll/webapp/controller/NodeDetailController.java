package com.webank.eggroll.webapp.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.entity.NodeDetail;
import com.webank.eggroll.webapp.entity.NodeInfo;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.NodeDetailQO;
import com.webank.eggroll.webapp.service.NodeDetailService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
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

        ResponseResult<Map<String, NodeDetail>> response;
        Map<String, NodeDetail> resources = null;
        int nodeNum = nodeDetailQO.getNodeNum();
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
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        // 将响应结果转换为 JSON 格式
        String json = JsonFormatUtil.toJson(response.getCode(),
                response.getMsg(), response.getData());

        resp.getWriter().write(json);
    }
}