package com.eggroll.core.containers.meta;

import com.eggroll.core.pojo.KillContainersRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class StartContainersResponse {
    Logger log = LoggerFactory.getLogger(StartContainersResponse.class);

    private String sessionId;

    public StartContainersResponse(String sessionId) {
        this.sessionId = sessionId;
    }

    public StartContainersResponse deserialize(ByteString byteString) {
        Containers.StartContainersResponse src = null;
        try {
            src = Containers.StartContainersResponse.parseFrom(byteString);
        } catch (InvalidProtocolBufferException e) {
            log.error("StartContainersResponse.deserialize() error :" ,e);
        }
        return new StartContainersResponse(src.getSessionId());
    }

    public ByteString serialize() {
        Containers.StartContainersResponse.Builder builder = Containers.StartContainersResponse.newBuilder();
        builder.setSessionId(this.sessionId);
        return builder.build().toByteString();
    }
}