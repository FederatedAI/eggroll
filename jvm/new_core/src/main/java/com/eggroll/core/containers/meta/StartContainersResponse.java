package com.eggroll.core.containers.meta;

import com.eggroll.core.pojo.KillContainersRequest;
import com.eggroll.core.pojo.RpcMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class StartContainersResponse implements RpcMessage {
    Logger log = LoggerFactory.getLogger(StartContainersResponse.class);

    private String sessionId;

    public StartContainersResponse() {
    }

    public byte[] serialize() {
        Containers.StartContainersResponse.Builder builder = Containers.StartContainersResponse.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.StartContainersResponse response = Containers.StartContainersResponse.parseFrom(data);
            this.sessionId = response.getSessionId();
        } catch (InvalidProtocolBufferException e) {
            log.error("StartContainersResponse.deserialize() error :", e);
        }
    }
}