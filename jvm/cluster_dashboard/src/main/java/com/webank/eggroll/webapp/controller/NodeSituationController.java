package com.webank.eggroll.webapp.controller;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.entity.NodeInfo;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.service.NodeSituationService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@Singleton
public class NodeSituationController extends HttpServlet {
    //获取节点（机器）概况
    private NodeSituationService situationService;

    @Inject
    public NodeSituationController(NodeSituationService situationService) {
        this.situationService = situationService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {


        ResponseResult<List<NodeInfo>> response;
        List<NodeInfo> nodeInfos = situationService.getNodeDetails();
        if (nodeInfos != null && !nodeInfos.isEmpty()) {
            // 获取数据成功
            response = ResponseResult.success(nodeInfos);
        } else {
            // 获取数据失败或无数据
            response = new ResponseResult(ErrorCode.DATA_ERROR);
        }
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        // 将响应结果转换为 JSON 格式
        String json = JsonFormatUtil.toJson(response.getCode(),
                response.getMsg(), response.getData());

        resp.getWriter().write(json);
    }

}