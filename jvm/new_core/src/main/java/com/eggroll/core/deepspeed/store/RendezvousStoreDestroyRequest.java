package com.eggroll.core.deepspeed.store;

import com.eggroll.core.pojo.RpcMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreDestroyRequest implements RpcMessage {
    private String prefix;

    @Override
    public byte[] serialize() {
        Deepspeed.StoreDestroyRequest.Builder builder = Deepspeed.StoreDestroyRequest.newBuilder()
                .setPrefix(this.getPrefix());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data){
        try {
            Deepspeed.StoreDestroyRequest src = Deepspeed.StoreDestroyRequest.parseFrom(data);
            this.prefix = src.getPrefix();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}