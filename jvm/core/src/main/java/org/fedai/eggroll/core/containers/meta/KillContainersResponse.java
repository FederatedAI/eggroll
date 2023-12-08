package org.fedai.eggroll.core.containers.meta;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.fedai.eggroll.core.pojo.RpcMessage;


@Data

public class KillContainersResponse implements RpcMessage {


    private String sessionId;

    @Override
    public byte[] serialize() {
        return Containers.KillContainersResponse.newBuilder()
                .setSessionId(this.sessionId).build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {

        try {
            Containers.KillContainersResponse proto = Containers.KillContainersResponse.parseFrom(data);
            this.setSessionId(proto.getSessionId());

        } catch (Exception e) {

        }

    }
}
