package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class KillJobResponse implements RpcMessage {

    Logger log = LoggerFactory.getLogger(KillJobResponse.class);

    private String sessionId;

    @Override
    public byte[] serialize() {
        Deepspeed.KillJobResponse.Builder builder = Deepspeed.KillJobResponse.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.KillJobResponse killJobResponse = Deepspeed.KillJobResponse.parseFrom(data);
            this.sessionId = killJobResponse.getSessionId();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
