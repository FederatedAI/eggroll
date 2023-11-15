package com.eggroll.core.deepspeed.store;

import com.eggroll.core.pojo.RpcMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreSetRequest implements RpcMessage {
    private String prefix;
    private byte[] key;
    private byte[] value;

    @Override
    public byte[] serialize() {
        Deepspeed.StoreSetRequest.Builder builder = Deepspeed.StoreSetRequest.newBuilder()
                .setPrefix(this.getPrefix())
                .setKey(ByteString.copyFrom(this.getKey()));
        builder.setValue(ByteString.copyFrom(this.getValue()));
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        Deepspeed.StoreSetRequest proto = null;
        try {
            proto = Deepspeed.StoreSetRequest.parseFrom(data);
            this.prefix = proto.getPrefix();
            this.key = proto.getKey().toByteArray();
            this.value = proto.getValue().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

    }

}