package com.eggroll.core.context;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
@Data
public class Context {

    boolean needDispatchResource =false;

    Map dataMap = new HashMap<String,Object>();
    public Object  getData(String key){
        return   dataMap.get(key);
    };

    public  void putData(String key, Object  data){
        this.dataMap.put(key,data);
    }
}
