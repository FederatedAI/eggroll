package org.fedai.eggroll.webapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.webapp.dao.service.UserService;
import org.fedai.eggroll.webapp.entity.UserDTO;
import org.fedai.eggroll.webapp.global.ErrorCode;
import org.fedai.eggroll.webapp.interfaces.ApiMethod;
import org.fedai.eggroll.webapp.model.ResponseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@Singleton
public class UserController {

    Logger logger = LoggerFactory.getLogger(UserController.class);

    @Inject
    private UserService userService;

    @ApiMethod("/eggroll/getPublicKey")
    public Object getPublicKey(HttpServletRequest req) throws Exception {

        String result = userService.getCurrentPublicKey();
        if (result != null) {
            return ResponseResult.success(result);
        } else {
            return ResponseResult.error(ErrorCode.SYS_ERROR.getCode(), "get public key failed");
        }
    }

    @ApiMethod("/eggroll/login")
    public Object login(HttpServletRequest req) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        UserDTO userDTO = objectMapper.readValue(req.getInputStream(), UserDTO.class);
        Boolean result = userService.login(userDTO, req);
        if (result) {
            return new ResponseResult(ErrorCode.SUCCESS, true);
        } else {
            return new ResponseResult(ErrorCode.LOGIN_ERROR, false);
        }
    }

    @ApiMethod("/eggroll/logout")
    public Object logout(HttpServletRequest req) {
        userService.logout(req);
        return new ResponseResult(ErrorCode.SUCCESS);
    }

}
