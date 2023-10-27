package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class KillContainersRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(KillContainersRequest.class);

    private String sessionId;
    private List<Long> containers;

    public Containers.KillContainersRequest toProto() {
        Containers.KillContainersRequest.Builder builder = Containers.KillContainersRequest.newBuilder();
        if (this.sessionId != null) {
            builder.setSessionId(sessionId);
        }
        if (CollectionUtils.isNotEmpty(this.containers)) {
            builder.addAllContainerIds(this.containers);
        }
        return builder.build();
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {

        try {
            Containers.KillContainersRequest proto = Containers.KillContainersRequest.parseFrom(data);
            this.setSessionId(proto.getSessionId());
            this.setContainers(proto.getContainerIdsList());
        } catch (Exception e) {
            log.error("KillContainersRequest.deserialize() error :", e);
        }

    }

}
