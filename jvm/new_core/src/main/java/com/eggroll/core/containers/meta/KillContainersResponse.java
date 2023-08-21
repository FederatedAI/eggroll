package com.eggroll.core.containers.meta;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class KillContainersResponse {

    Logger log = LoggerFactory.getLogger(KillContainersResponse.class);
    private String sessionId;

    public KillContainersResponse deserialize(byte[] bytes){
        KillContainersResponse killContainersResponse = new KillContainersResponse();
        try {
            Containers.KillContainersResponse proto = Containers.KillContainersResponse.parseFrom(bytes);
            killContainersResponse.setSessionId(proto.getSessionId());

        }catch (Exception e){
            log.error("KillContainersResponse.deserialize() error :" ,e);
        }
        return killContainersResponse;
    }

    public byte[] serialize(){
       return Containers.KillContainersResponse.newBuilder()
                .setSessionId(this.sessionId).build().toByteArray();
    }
}
