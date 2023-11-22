package org.fedai.eggroll.webapp.controller;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.invoke.InvokeInfo;
import org.fedai.eggroll.webapp.global.ErrorCode;
import org.fedai.eggroll.webapp.intercept.UserInterceptor;
import org.fedai.eggroll.webapp.interfaces.ApiMethod;
import org.fedai.eggroll.webapp.model.ResponseResult;
import org.fedai.eggroll.webapp.utils.JsonFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.*;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class DispatcherServlet extends HttpServlet {

    Logger logger = LoggerFactory.getLogger(DispatcherServlet.class);

    private ConcurrentHashMap<String, InvokeInfo> uriMap = new ConcurrentHashMap();

    private UserInterceptor userInterceptor;

    @Inject
    public DispatcherServlet(UserInterceptor interceptor) {
        this.userInterceptor = interceptor;
    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ResponseResult<Object> response = new ResponseResult<>();

        String url = req.getRequestURI();
        InvokeInfo invokeInfo = uriMap.get(url);

        if (invokeInfo == null) {
            throw new ServletException("No API found for: " + url);
        }
        if (!url.contains("login")&& !url.contains("getPublicKey")){
            // 拦截器拦截
            if (!userInterceptor.intercept(req, resp)) {
                // 未登录返回错误码
                String json = JsonFormatUtil.toJson(ErrorCode.NO_LOGIN.getCode(),
                        ErrorCode.NO_LOGIN.getMsg(),false);
                resp.getWriter().write(json);
                return;
            }
        }
        try {
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

            if (url.contains("login") && ((Boolean) ((ResponseResult) result).getData())) {
                HttpSession session = req.getSession();
                // 设置session的过期时间为一小时后（以秒为单位）
                int interval = MetaInfo.EGGROLL_SESSION_EXPIRED_TIME * 60;
                session.setMaxInactiveInterval(interval);
            }
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