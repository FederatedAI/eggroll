package com.webank.eggroll.webapp.controller;

import org.fedai.eggroll.core.invoke.InvokeInfo;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.interfaces.ApiMethod;
import com.webank.eggroll.webapp.model.ResponseResult;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class DispatcherServlet extends HttpServlet {

    Logger logger = LoggerFactory.getLogger(DispatcherServlet.class);

    private ConcurrentHashMap<String, InvokeInfo> uriMap = new ConcurrentHashMap();

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String url = req.getRequestURI();
        InvokeInfo invokeInfo = uriMap.get(url);

        if (invokeInfo == null) {
            throw new ServletException("No API found for: " + url);
        }
        ResponseResult<Object> response = new ResponseResult<>();
        try {
//            invokeInfo.getMethod().invoke(invokeInfo.getObject(),
//            objectMapper.readValue(req.getInputStream(), invokeInfo.getParamClass()))
            Object result = invokeInfo.getMethod().invoke(invokeInfo.getObject(), req);
            if (result != null) {
                if (result instanceof ResponseResult) {
                    response = (ResponseResult<Object>) result;
                } else {
                    response = ResponseResult.success(result);
                }
            } else {
                response = ResponseResult.noData(ErrorCode.NO_DATA);
            }
            //转化成json的操作
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            String json = JsonFormatUtil.toJson(response.getCode(),
                    response.getMsg(), response.getData());
            resp.getWriter().write(json);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ServletException(e);
        }
    }

    private void doRegister(String uri, Object service, Method method, Class paramClass) {
        InvokeInfo invokeInfo = new InvokeInfo(uri, service, method, paramClass);
        this.uriMap.put(uri, invokeInfo);
    }

    public void register(Object service) {
        Method[] methods;
        if (service instanceof Class) {
            methods = ((Class) service).getMethods();
        } else {
            methods = service.getClass().getMethods();
        }
        for (Method method : methods) {
            ApiMethod uri = method.getDeclaredAnnotation(ApiMethod.class);
            if (uri != null) {
                Class[] types = method.getParameterTypes();
                if (types.length > 0) {
                    doRegister(uri.value(), service, method, types[0]);
                } else {
                    doRegister(uri.value(), service, method, null);
                }
            }
        }
    }
}