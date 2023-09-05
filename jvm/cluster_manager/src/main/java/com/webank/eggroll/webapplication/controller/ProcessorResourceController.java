package com.webank.eggroll.webapplication.controller;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapplication.model.CommonResponse;
import com.webank.eggroll.webapplication.service.ProcessorResourceServiceN;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
@Singleton
public class ProcessorResourceController extends HttpServlet {
    @Inject
    private ProcessorResourceServiceN resourceService;

    @Inject
    public ProcessorResourceController(ProcessorResourceServiceN resourceService) {
        this.resourceService = resourceService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//        int page = Integer.parseInt(req.getParameter("page"));
//        int size = Integer.parseInt(req.getParameter("size"));

        CommonResponse<List<ProcessorResource>> response;
        List<ProcessorResource> resources = resourceService.getAllResources();
        if (resources != null && !resources.isEmpty()) {
            // 获取数据成功
            response = CommonResponse.success(resources);
        } else {
            // 获取数据失败或无数据
            response = CommonResponse.error("Failed to retrieve resources.");
        }

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        // 将响应结果转换为 JSON 格式
        String json;
        if (CommonResponse.ifSuccess(response)) {
            // 获取数据成功
            json = new Gson().toJson(response.getData());
        } else {
            // 获取数据失败或没有数据
            json = new Gson().toJson(response.getMsg());
        }
        resp.getWriter().write(json);
    }
}
