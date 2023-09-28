package com.eggroll.core.deepspeed.store;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;

public class RendezvousStoreSetResponse {
    public static byte[] serialize(RendezvousStoreSetResponse src) {
        Deepspeed.StoreSetResponse.Builder builder = Deepspeed.StoreSetResponse.newBuilder();
        return builder.build().toByteArray();
    }

    public static RendezvousStoreSetResponse deserialize(byte[] bytes) throws InvalidProtocolBufferException {
        Deepspeed.StoreSetResponse proto = Deepspeed.StoreSetResponse.parseFrom(bytes);
        return new RendezvousStoreSetResponse();
    }
}