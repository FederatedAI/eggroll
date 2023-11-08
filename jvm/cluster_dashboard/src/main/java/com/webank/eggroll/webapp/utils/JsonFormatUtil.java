package com.webank.eggroll.webapp.utils;

import com.github.pagehelper.PageInfo;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonFormatUtil {

    public static <T> String toJson(int code, String msg, T data) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("code", code);
        resultMap.put("msg", msg);
        if (data == null) {
            resultMap.put("data", new ArrayList());
            resultMap.put("total", 0);
            resultMap.put("current", 0);
        } else if (data instanceof Map) {
            resultMap.put("data", data);
            resultMap.put("current",((Map) data).size());
            resultMap.put("total", ((Map) data).size());
        } else if (data instanceof List) {
            resultMap.put("data", data);
            resultMap.put("current",((List) data).size());
            resultMap.put("total", ((List) data).size());
        } else if (data instanceof PageInfo) {
            resultMap.put("data", ((PageInfo<?>) data).getList());
            resultMap.put("current",((PageInfo<?>) data).getSize());
            resultMap.put("total", ((PageInfo<?>) data).getTotal());
        }else {
            resultMap.put("data", data);
            resultMap.put("current", 1);
            resultMap.put("total", 1);
        }
        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }

}