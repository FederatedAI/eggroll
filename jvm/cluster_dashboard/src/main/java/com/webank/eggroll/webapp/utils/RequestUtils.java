package com.webank.eggroll.webapp.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webank.eggroll.webapp.entity.UserDTO;
import com.webank.eggroll.webapp.global.ErrorCode;
import com.webank.eggroll.webapp.model.ResponseResult;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public class RequestUtils {

    public static UserDTO extractUserCredentials(HttpServletRequest request) throws IOException {
        StringBuilder requestBody = new StringBuilder();
        String line;

        // 从请求中获取原始请求体数据
        BufferedReader reader = request.getReader();
        while ((line = reader.readLine()) != null) {
            requestBody.append(line);
        }

        // 使用 Jackson ObjectMapper 解析 JSON
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(requestBody.toString());

        // 提取 username 和 password
        String username = jsonNode.get("username").asText();
        String password = jsonNode.get("password").asText();

        return new UserDTO(username, password);
    }

    public static <T> ResponseResult<List<T>> isResourceExist(List<T> resources) {
        ResponseResult<List<T>> response;
        if (resources != null && !resources.isEmpty()) {
            response = ResponseResult.success(resources);
        } else {
            response = new ResponseResult<>(ErrorCode.DATA_ERROR);
        }
        return response;
    }


}