package com.webank.eggroll.webapplication.controller;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapplication.service.ProcessorResourceService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

public class ProcessorResourceController extends HttpServlet {
    private final ProcessorResourceService resourceService;

    @Inject
    public ProcessorResourceController(ProcessorResourceService resourceService) {
        this.resourceService = resourceService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        int page = Integer.parseInt(req.getParameter("page"));
        int size = Integer.parseInt(req.getParameter("size"));

        List<ProcessorResource> resources = resourceService.getAllResources(page, size);

        // 将结果转换为 JSON 格式
        String json = new Gson().toJson(resources);

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        resp.getWriter().write(json);
    }
}
