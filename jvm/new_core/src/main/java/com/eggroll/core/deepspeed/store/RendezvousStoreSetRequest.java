package com.eggroll.core.deepspeed.store;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreSetRequest {
    private String prefix;
    private byte[] key;
    private byte[] value;

    public byte[] serialize() {
        Deepspeed.StoreSetRequest.Builder builder = Deepspeed.StoreSetRequest.newBuilder()
                .setPrefix(this.getPrefix())
                .setKey(ByteString.copyFrom(this.getKey()));
        builder.setValue(ByteString.copyFrom(this.getValue()));
        return builder.build().toByteArray();
    }

    public void deserialize(byte[] data) throws InvalidProtocolBufferException {
        Deepspeed.StoreSetRequest proto = Deepspeed.StoreSetRequest.parseFrom(data);
        this.prefix = proto.getPrefix();
        this.key = proto.getKey().toByteArray();
        this.value = proto.getValue().toByteArray();
    }
}