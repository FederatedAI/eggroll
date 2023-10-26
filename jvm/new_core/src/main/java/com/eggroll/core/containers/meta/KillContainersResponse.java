package com.eggroll.core.containers.meta;

import com.eggroll.core.pojo.RpcMessage;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

//    public KillContainersResponse deserialize(byte[] bytes){
//        KillContainersResponse killContainersResponse = new KillContainersResponse();
//        try {
//            Containers.KillContainersResponse proto = Containers.KillContainersResponse.parseFrom(bytes);
//            killContainersResponse.setSessionId(proto.getSessionId());
//
//        }catch (Exception e){
//            log.error("KillContainersResponse.deserialize() error :" ,e);
//        }
//        return killContainersResponse;
//    }
//
//    public byte[] serialize(){
//       return Containers.KillContainersResponse.newBuilder()
//                .setSessionId(this.sessionId).build().toByteArray();
//    }
}
