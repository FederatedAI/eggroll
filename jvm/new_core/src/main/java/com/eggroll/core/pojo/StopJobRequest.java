package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class StopJobRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(StopJobRequest.class);
    private String sessionId;

    @Override
    public byte[] serialize() {
        Deepspeed.StopJobRequest.Builder builder = Deepspeed.StopJobRequest.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StopJobRequest protoEntity = Deepspeed.StopJobRequest.parseFrom(data);
            this.sessionId = protoEntity.getSessionId();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
