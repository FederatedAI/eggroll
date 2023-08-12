package com.eggroll.core.context;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
@Data
public class Context {

    String  actionType;

    long startTimestamp =  System.currentTimeMillis();

    boolean needDispatchResource =false;

    String returnCode;

    String returnMsg;

    Map dataMap = new HashMap<String,Object>();

    public Object  getData(String key){
        return   dataMap.get(key);
    };

    public  void putData(String key, Object  data){
        this.dataMap.put(key,data);
    }

    public String toString(){
        StringBuffer stringBuffer = new StringBuffer();

        if (this.getActionType() != null) {
            stringBuffer.append(this.getActionType()).append(SPLIT);
        }
        if (this.getReturnCode() != null) {
            stringBuffer.append("code:").append(this.getReturnCode()).append(SPLIT);
        }
        stringBuffer.append("cost:").append(System.currentTimeMillis() - this.getStartTimestamp()).append(SPLIT);
        if (this.getReturnMsg() != null) {
            stringBuffer.append("msg:").append(this.getReturnMsg());
        }
        return  stringBuffer.toString();
    }
    static final String SPLIT= "|";
}
