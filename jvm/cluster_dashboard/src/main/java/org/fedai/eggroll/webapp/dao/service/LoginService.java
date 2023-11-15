package org.fedai.eggroll.webapp.dao.service;

import com.webank.eggroll.webapp.entity.UserInfo;
import org.fedai.eggroll.webapp.utils.RequestUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public class LoginService {

    public boolean login(UserInfo userInfo, HttpServletRequest req){
        try {
            UserInfo userCredentials = RequestUtils.extractUserCredentials(req);
            String username = userCredentials.getUsername();
            String password = userCredentials.getPassword();
            if (username == null || password == null) {
                return false;
            }
            if (!username.equals(userInfo.getUsername()) || !password.equals(userInfo.getPassword())) {
                return false;
            }else {

                HttpSession session = req.getSession(false);
                if (session != null) {
                    // 会话存在，可以设置属性
                    session.setAttribute("username", username);
                } else {
                    // 会话不存在，可以选择创建会话
                    session = req.getSession(true);
                    session.setAttribute("username", username);
                }
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
