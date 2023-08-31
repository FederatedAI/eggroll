package com.eggroll.core.context;

import com.eggroll.core.pojo.RpcMessage;
import com.eggroll.core.utils.JsonUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
@Data
public class Context {

    String processorId;

    String sessionId;

    String nodeId;

    String  actionType;

    long startTimestamp =  System.currentTimeMillis();

    boolean needDispatchResource =false;

    String returnCode;

    String returnMsg;

    Map<String, String> options;

    RpcMessage request;

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
        if(StringUtils.isNotEmpty(sessionId)){
            stringBuffer.append("session:").append(sessionId).append(SPLIT);
        }
        if(StringUtils.isNotEmpty(processorId)){
            stringBuffer.append("processorId:").append(processorId).append(SPLIT);
        }
        if(StringUtils.isNotEmpty(nodeId)){
            stringBuffer.append("nodeId:").append(nodeId).append(SPLIT);
        }

        if(options!=null){
            stringBuffer.append("option:").append(JsonUtil.object2Json(options)).append(SPLIT);
        }
        if(request!=null){
            stringBuffer.append("").append(request.toString()).append(SPLIT);
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
