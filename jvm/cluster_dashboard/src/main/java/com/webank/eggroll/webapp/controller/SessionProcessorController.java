package com.webank.eggroll.webapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.webapp.dao.SessionProcessorDao;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@Singleton
public class SessionProcessorController extends HttpServlet {
    private SessionProcessorDao resourceDao;

    @Inject
    public SessionProcessorController(SessionProcessorDao resourceDao) {
        this.resourceDao = resourceDao;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SessionProcessorQO sessionProcessorQO = objectMapper.readValue(req.getInputStream(), SessionProcessorQO.class);


        ResponseResult<List<SessionProcessor>> response;
        List<SessionProcessor> resources = resourceDao.queryData(sessionProcessorQO);
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
