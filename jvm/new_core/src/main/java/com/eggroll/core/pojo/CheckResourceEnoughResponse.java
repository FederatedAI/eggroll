package com.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.resource.Resources;
import lombok.Data;

@Data
public class CheckResourceEnoughResponse implements RpcMessage {

    private boolean isEnough = false;

    @Override
    public byte[] serialize() {
        Resources.CheckResourceEnoughResponse.Builder builder = Resources.CheckResourceEnoughResponse.newBuilder();
        builder.setIsEnough(isEnough);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Resources.CheckResourceEnoughResponse response = Resources.CheckResourceEnoughResponse.parseFrom(data);
            this.isEnough = response.getIsEnough();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}
