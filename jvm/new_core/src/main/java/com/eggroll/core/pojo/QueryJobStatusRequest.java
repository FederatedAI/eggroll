package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class QueryJobStatusRequest implements RpcMessage{
    Logger log = LoggerFactory.getLogger(QueryJobStatusRequest.class);
    private String sessionId;

    @Override
    public byte[] serialize() {
        Deepspeed.QueryJobStatusRequest.Builder builder = Deepspeed.QueryJobStatusRequest.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.QueryJobStatusRequest queryJobStatusRequest = Deepspeed.QueryJobStatusRequest.parseFrom(data);
            this.sessionId = queryJobStatusRequest.getSessionId();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
