package com.eggroll.core.context;

import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.RpcMessage;
import com.eggroll.core.utils.RandomUtil;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Data
public class Context {
    String seq = RandomUtil.getRandomNumString(4);
    String processorId;
    String sessionId;
    String nodeId;
    String uri;
    String actionType;
    long startTimestamp = System.currentTimeMillis();
    boolean needDispatchResource = false;
    String returnCode;
    String returnMsg;
    Map<String, String> options;
    RpcMessage request;
    ErEndpoint endpoint;
    Throwable throwable;
    String sourceIp;
    Map<String, String> logData = Maps.newHashMap();
    Map dataMap = new HashMap<String, Object>();

    public void putLogData(String key, String value) {
        logData.put(key, value);
    }

    public Object getData(String key) {
        return dataMap.get(key);
    }

    ;

    public void putData(String key, Object data) {
        this.dataMap.put(key, data);
    }

    @Override
    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();
        if (StringUtils.isNotEmpty(actionType)) {
            stringBuffer.append(actionType).append(SPLIT);
        }
        if (this.getUri() != null) {
            stringBuffer.append(this.getUri()).append(SPLIT);
        }
        if (StringUtils.isNotEmpty(sessionId)) {
            stringBuffer.append("session:").append(sessionId).append(SPLIT);
        }
        if (StringUtils.isNotEmpty(processorId)) {
            stringBuffer.append("processorId:").append(processorId).append(SPLIT);
        }
        if (StringUtils.isNotEmpty(nodeId)) {
            stringBuffer.append("nodeId:").append(nodeId).append(SPLIT);
        }
        if (logData.size() > 0) {
            logData.forEach((k, v) -> {
                stringBuffer.append(k).append(":").append(v).append(SPLIT);
            });
        }

//        if(options!=null){
//            stringBuffer.append("option:").append(JsonUtil.object2Json(options)).append(SPLIT);
//        }
//        if(request!=null){
//            stringBuffer.append("").append(request.toString()).append(SPLIT);
//        }
        if (this.getReturnCode() != null) {
            stringBuffer.append("code:").append(this.getReturnCode()).append(SPLIT);
        }
        final long cost = System.currentTimeMillis() - startTimestamp;
        stringBuffer.append("cost:").append(cost > 20 ? "WARNING<" + cost + ">" : cost).append(SPLIT);
        if (this.getReturnMsg() != null) {
            stringBuffer.append("msg:").append(this.getReturnMsg());
        }
        if (StringUtils.isNotEmpty(sourceIp)) {
            stringBuffer.append("from:").append(sourceIp).append(SPLIT);
        }
        if (endpoint != null) {
            stringBuffer.append("sendTo:").append(endpoint.toString()).append(SPLIT);
        }
        if (throwable != null) {
            stringBuffer.append("error:").append(throwable.toString()).append(SPLIT);
        }

        return stringBuffer.toString();
    }

    static final String SPLIT = "|";
}
