package com.webank.eggroll.webapp.utils;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonFormatUtil {

    public static String toJson(int code, String msg, List<?> data) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("code", code);
        resultMap.put("msg", msg);
        if (data == null || data.isEmpty()){
            resultMap.put("data", new ArrayList());
            resultMap.put("total", 0);
        }else {
            resultMap.put("data", data);
            resultMap.put("total", data.size());
        }
        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }

}