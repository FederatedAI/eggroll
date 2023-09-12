package com.webank.eggroll.webapp.controller;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.webapp.dao.NodeResourceDao;
import com.webank.eggroll.webapp.model.CommonResponse;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@Singleton
public class NodeResourceController extends HttpServlet {
    private NodeResourceDao resourceDao;

    @Inject
    public NodeResourceController(NodeResourceDao resourceDao) {
        this.resourceDao = resourceDao;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        int page = Integer.parseInt(req.getParameter("page"));
        int pageSize = Integer.parseInt(req.getParameter("pageSize"));

        CommonResponse<List<NodeResource>> response;
        List<NodeResource> resources = resourceDao.getData(page, pageSize);
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
        String json = JsonFormatUtil.toJson(response.getCode(),
                response.getMsg(), response.getData());

        resp.getWriter().write(json);
    }
}
