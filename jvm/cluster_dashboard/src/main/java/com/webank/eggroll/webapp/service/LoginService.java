package com.webank.eggroll.webapp.service;

import com.webank.eggroll.webapp.entity.UserInfo;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public class LoginService {


    public boolean login(UserInfo userInfo, HttpServletRequest req){
        String username = req.getParameter("username");
        String password = req.getParameter("password");
        if (username == null || password == null) {
            return false;
        }
        if (!username.equals(userInfo.getUsername()) || !password.equals(userInfo.getPassword())) {
            return false;
        }else {
//            req.getSession().setAttribute("username", username);
            HttpSession session = req.getSession(false); // 使用 getSession(false) 来获取当前会话，如果不存在则返回 null

            if (session != null) {
                // 会话存在，可以设置属性
                session.setAttribute("username", username);
            } else {
                // 会话不存在，可以选择创建会话
                session = req.getSession(true); // 使用 getSession(true) 来创建会话
                session.setAttribute("username", username);
            }

            return true;
        }
    }

}
