package org.fedai.eggroll.webapp.dao.service;


import com.google.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.webapp.entity.UserDTO;
import org.fedai.eggroll.webapp.utils.StandardRSAUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public class UserService {

    Logger logger = LoggerFactory.getLogger(UserService.class);

    @Inject
    private SecurityService securityService;

    public Boolean login(UserDTO userDTO, HttpServletRequest req) {
        String username = userDTO.getUsername();
        String password = userDTO.getPassword();

        if (!checkUser(username, password)) {
            return false;
        } else {
            HttpSession session = req.getSession();
            session.setAttribute("USER", userDTO);
            return true;
        }
    }

    public boolean checkUser(String username, String password) {

        // 更新配置文件的方法待定updateConfig();
        String usernameValue = MetaInfo.USERNAME;
        String passwordValue = MetaInfo.PASSWORD;
        String privateKey = MetaInfo.ENCRYPT_PRIVATE_KEY;
        String encrypted = MetaInfo.ENCRYPT_ENABLE;
        if (StringUtils.isNotBlank(privateKey) && "true".equalsIgnoreCase(encrypted)) {
            try {
                passwordValue = StandardRSAUtils.decryptByPrivateKey(passwordValue, privateKey);
            } catch (Exception e) {
                logger.error("decrypt password error");
                return false;
            }
        }

        if (!username.equals(usernameValue)) {
            return false;
        }
        try {
            return securityService.compareValue(password, passwordValue);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("decrypt password error");
            return false;
        }
    }

}
