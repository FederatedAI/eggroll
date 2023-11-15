package com.webank.eggroll.webapp.controller;

import org.fedai.eggroll.core.config.MetaInfo;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.entity.UserInfo;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.dao.service.LoginService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Singleton
public class LoginController extends HttpServlet{

    private final LoginService loginService;

    @Inject
    private LoginController(LoginService loginService) {
        this.loginService = loginService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        //从MetaInfo里面获取用户名和密码，然后封装到用户实体类里面
        UserInfo userInfo = new UserInfo(MetaInfo.USERNAME, MetaInfo.PASSWORD);
        boolean result = loginService.login(userInfo, req);
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        String json;
        if (result){
            // 将响应结果转换为 JSON 格式
            json = JsonFormatUtil.toJson(ErrorCode.SUCCESS.getCode(),
                    ErrorCode.SUCCESS.getMsg(), true);
        } else {
            // 将响应结果转换为 JSON 格式
            json = JsonFormatUtil.toJson(ErrorCode.LOGIN_FAILED.getCode(),
                    ErrorCode.LOGIN_FAILED.getMsg(), false);
        }
        resp.getWriter().write(json);


    }
}
