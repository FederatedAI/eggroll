package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class KillJobRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(KillJobRequest.class);
    private String sessionId;

    @Override
    public byte[] serialize() {
        Deepspeed.KillJobRequest.Builder builder = Deepspeed.KillJobRequest.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.KillJobRequest killJobRequest = Deepspeed.KillJobRequest.parseFrom(data);
            this.sessionId = killJobRequest.getSessionId();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
