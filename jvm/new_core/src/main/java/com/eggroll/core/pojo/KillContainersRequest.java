package com.eggroll.core.pojo;

import com.google.protobuf.ByteString;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class KillContainersRequest  implements RpcMessage{

    Logger log = LoggerFactory.getLogger(KillContainersRequest.class);

    private String sessionId;
    private List<Long> containers;

    @Override
    public byte[] serialize() {
                return Containers.KillContainersResponse.newBuilder()
                .setSessionId(this.getSessionId()).build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {

        try {
            Containers.KillContainersRequest proto = Containers.KillContainersRequest.parseFrom(data);
            this.setSessionId(proto.getSessionId());
            this.setContainers(proto.getContainerIdsList());
        }catch (Exception e){
            log.error("KillContainersRequest.deserialize() error :" ,e);
        }

    }

//    public KillContainersRequest deserialize(byte[] bytes){
//        KillContainersRequest killContainersRequest = new KillContainersRequest();
//        try {
//            Containers.KillContainersRequest proto = Containers.KillContainersRequest.parseFrom(bytes);
//            killContainersRequest.setSessionId(proto.getSessionId());
//            killContainersRequest.setContainers(proto.getContainerIdsList());
//        }catch (Exception e){
//            log.error("KillContainersRequest.deserialize() error :" ,e);
//        }
//        return killContainersRequest;
//    }
//
//    public byte[] serialize(){
//        return Containers.KillContainersResponse.newBuilder()
//                .setSessionId(this.getSessionId()).build().toByteArray();
//    }
}
