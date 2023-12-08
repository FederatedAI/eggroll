package org.fedai.eggroll.webapp.intercept;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface UserInterceptor {
    Boolean intercept(HttpServletRequest request , HttpServletResponse response);
}
