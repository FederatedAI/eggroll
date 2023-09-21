package com.webank.eggroll.webapp.service;

import com.webank.eggroll.webapp.entity.UserInfo;

import javax.servlet.http.HttpServletRequest;

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
            req.getSession().setAttribute("username", username);
            return true;
        }
    }

}
