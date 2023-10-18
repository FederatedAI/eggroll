package com.eggroll.core.deepspeed.store;

import com.eggroll.core.pojo.RpcMessage;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreSetResponse implements RpcMessage {

    @Override
    public byte[] serialize() {
        Deepspeed.StoreSetResponse.Builder builder = Deepspeed.StoreSetResponse.newBuilder();
        return builder.build().toByteArray();
    }

    public void deserialize(byte[] data) {
    }
}