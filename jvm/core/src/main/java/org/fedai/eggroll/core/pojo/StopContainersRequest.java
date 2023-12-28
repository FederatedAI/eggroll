package org.fedai.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class StopContainersRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(StopContainersRequest.class);

    private String sessionId;
    private List<Long> containers;

    public StopContainersRequest(String sessionId, List<Long> containers) {
        this.sessionId = sessionId;
        this.containers = containers;
    }


    @Override
    public byte[] serialize() {
        Containers.StopContainersRequest.Builder builder = Containers.StopContainersRequest.newBuilder();
        builder.setSessionId(this.sessionId);
        builder.addAllContainerIds(this.containers);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.StopContainersRequest proto = Containers.StopContainersRequest.parseFrom(data);
            this.sessionId = proto.getSessionId();
            this.containers = proto.getContainerIdsList();
        } catch (InvalidProtocolBufferException e) {
            log.error("StopContainersRequest.deserialize() error :", e);
        }
    }
}