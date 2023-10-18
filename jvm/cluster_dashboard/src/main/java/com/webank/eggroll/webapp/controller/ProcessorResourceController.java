package com.webank.eggroll.webapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.webapp.dao.ProcessorResourceDao;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.queryobject.ProcessorResourceQO;
import com.webank.eggroll.webapp.queryobject.SessionProcessorQO;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@Singleton
public class ProcessorResourceController extends HttpServlet {

    private ProcessorResourceDao resourceDao;

    @Inject
    public ProcessorResourceController(ProcessorResourceDao resourceDao) {
        this.resourceDao = resourceDao;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ProcessorResourceQO processorResourceQO = objectMapper.readValue(req.getInputStream(), ProcessorResourceQO.class);

        ResponseResult<List<ProcessorResource>> response;
        List<ProcessorResource> resources = resourceDao.queryData(processorResourceQO);
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
