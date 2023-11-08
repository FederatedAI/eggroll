package com.webank.eggroll.webapp.controller;

import com.webank.eggroll.webapp.dao.service.ZookeeperQueryService;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@Singleton
public class ZookeeperQueryResource extends HttpServlet {

    private final ZookeeperQueryService zookeeperQueryService;

    @Inject
    public ZookeeperQueryResource(ZookeeperQueryService zookeeperQueryService) {
        this.zookeeperQueryService = zookeeperQueryService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {

        try {
            String nodePath = req.getParameter("nodePath");
            String result = zookeeperQueryService.queryNodeInfo(nodePath);
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            resp.getWriter().write(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
