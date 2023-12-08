package org.fedai.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;

@Data
public class CheckResourceEnoughResponse implements RpcMessage {

    private boolean isEnough = false;

    private ServerCluster serverCluster = new ServerCluster();

    @Override
    public byte[] serialize() {
        Meta.CheckResourceEnoughResponse.Builder builder = Meta.CheckResourceEnoughResponse.newBuilder();
        builder.setIsEnough(isEnough);
        builder.setClusterInfo(serverCluster.toProto());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.CheckResourceEnoughResponse response = Meta.CheckResourceEnoughResponse.parseFrom(data);
            this.isEnough = response.getIsEnough();
            this.serverCluster = serverCluster.fromProto(response.getClusterInfo());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}
