package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class QueryJobStatusResponse implements RpcMessage{
    Logger log = LoggerFactory.getLogger(QueryJobStatusResponse.class);
    private String sessionId;
    private String status;

    @Override
    public byte[] serialize() {
        Deepspeed.QueryJobStatusResponse.Builder builder = Deepspeed.QueryJobStatusResponse.newBuilder();
        builder.setSessionId(this.sessionId)
                .setStatus(this.status);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.QueryJobStatusResponse queryJobStatusResponse = Deepspeed.QueryJobStatusResponse.parseFrom(data);
            this.sessionId = queryJobStatusResponse.getSessionId();
            this.status = queryJobStatusResponse.getStatus();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
