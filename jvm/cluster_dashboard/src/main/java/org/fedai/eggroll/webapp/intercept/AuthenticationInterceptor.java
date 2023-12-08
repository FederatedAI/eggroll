package org.fedai.eggroll.webapp.intercept;

import com.google.inject.Inject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class AuthenticationInterceptor implements UserInterceptor{

    @Override
    public Boolean intercept(HttpServletRequest request, HttpServletResponse response) {
        HttpSession session = request.getSession(false);
        if (session == null) {
            return false;
        }
        Object user = session.getAttribute("USER");
        if (user == null) {
            return false;
        }
        return true;
    }
}
