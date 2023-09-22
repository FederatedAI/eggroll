package com.webank.eggroll.webapp.model;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class MyHttpSessionListener implements HttpSessionListener {

    @Override
    public void sessionCreated(HttpSessionEvent se) {
        // 当 HttpSession 被创建时触发
        System.out.println("Session Created: " + se.getSession().getId());
    }

    @Override
    public void sessionDestroyed(HttpSessionEvent se) {
        // 当 HttpSession 被销毁时触发
        System.out.println("Session Destroyed: " + se.getSession().getId());
    }
}
