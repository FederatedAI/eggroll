package org.fedai.eggroll.core.containers.meta;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class StopContainersResponse {

    Logger log = LoggerFactory.getLogger(StopContainersResponse.class);

    private String sessionId;

    public StopContainersResponse(String sessionId) {
        this.sessionId = sessionId;
    }

    public StopContainersResponse deserialize(ByteString byteString) {
        Containers.StopContainersResponse proto = null;
        try {
            proto = Containers.StopContainersResponse.parseFrom(byteString);
        } catch (InvalidProtocolBufferException e) {
            log.error("StopContainersResponse.deserialize() error :", e);
        }
        return new StopContainersResponse(proto.getSessionId());
    }

    public byte[] serialize() {
        Containers.StopContainersResponse.Builder builder = Containers.StopContainersResponse.newBuilder()
                .setSessionId(this.sessionId);
        return builder.build().toByteArray();
    }
}