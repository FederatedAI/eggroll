package com.eggroll.core.deepspeed.store;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreDestroyRequest {
    private String prefix;

    public RendezvousStoreDestroyRequest(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    public static byte[] serialize(RendezvousStoreDestroyRequest src) {
        Deepspeed.StoreDestroyRequest.Builder builder = Deepspeed.StoreDestroyRequest.newBuilder()
                .setPrefix(src.getPrefix());
        return builder.build().toByteArray();
    }

    public static RendezvousStoreDestroyRequest deserialize(ByteString byteString) throws InvalidProtocolBufferException {
        Deepspeed.StoreDestroyRequest src = Deepspeed.StoreDestroyRequest.parseFrom(byteString);
        String prefix = src.getPrefix();
        return new RendezvousStoreDestroyRequest(prefix);
    }
}