package com.webank.eggroll.webapp.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webank.eggroll.webapp.entity.UserInfo;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;

public class RequestUtils {

    public static UserInfo extractUserCredentials(HttpServletRequest request) throws IOException {
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

        return new UserInfo(username, password);
    }

}