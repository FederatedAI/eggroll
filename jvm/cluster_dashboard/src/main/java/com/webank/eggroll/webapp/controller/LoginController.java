package com.webank.eggroll.webapp.controller;
import com.google.inject.Singleton;
import com.webank.eggroll.webapp.service.LoginService;

import javax.servlet.http.HttpServlet;

@Singleton
public class LoginController extends HttpServlet{

    private LoginService loginService;

}
