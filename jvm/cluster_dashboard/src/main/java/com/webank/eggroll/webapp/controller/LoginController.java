package com.webank.eggroll.webapp.controller;
import com.eggroll.core.config.MetaInfo;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.entity.UserInfo;
import com.webank.eggroll.webapp.exception.ErrorCode;
import com.webank.eggroll.webapp.service.LoginService;
import com.webank.eggroll.webapp.utils.JsonFormatUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Singleton
public class LoginController extends HttpServlet{

    private LoginService loginService;

    @Inject
    private LoginController(LoginService loginService) {
        this.loginService = loginService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        //从MetaInfo里面获取用户名和密码，然后封装到用户实体类里面
        UserInfo userInfo = new UserInfo();
        userInfo.setUsername(MetaInfo.USERNAME);
        userInfo.setPassword(MetaInfo.PASSWORD);

        boolean result = loginService.login(userInfo, req);
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        if (result){
            // 将响应结果转换为 JSON 格式
            String json = JsonFormatUtil.toJson(ErrorCode.SUCCESS.getCode(),
                    ErrorCode.SUCCESS.getMsg(), true);
            resp.getWriter().write(json);
        }






    }
}
