package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class QueryJobRequest implements RpcMessage{
    Logger log = LoggerFactory.getLogger(QueryJobRequest.class);
    private String sessionId;

    @Override
    public byte[] serialize() {
        Deepspeed.QueryJobRequest.Builder builder = Deepspeed.QueryJobRequest.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.QueryJobRequest request = Deepspeed.QueryJobRequest.parseFrom(data);
            this.sessionId = request.getSessionId();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
