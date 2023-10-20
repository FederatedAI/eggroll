package com.webank.eggroll.webapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.webapp.dao.SessionMainDao;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.ServerNodeQO;
import com.webank.eggroll.webapp.queryobject.SessionMainQO;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SessionMainController extends HttpServlet {
    private SessionMainDao sessionMainDao;
    @Inject
    public SessionMainController(SessionMainDao sessionMainDao) {
        this.sessionMainDao = sessionMainDao;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionMainQO sessionMainQO = objectMapper.readValue(req.getInputStream(), SessionMainQO.class);

        ResponseResult<List<SessionMain>> response;
        List<SessionMain> resources = new ArrayList<>();
        Integer topCount = sessionMainQO.getTopCount();
        if (topCount > 0) {
            resources = sessionMainDao.topQuery(topCount);
        } else {
            resources = sessionMainDao.queryData(sessionMainQO);
        }


        if (resources != null && !resources.isEmpty()) {
            // 获取数据成功
            response = ResponseResult.success(resources);
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
