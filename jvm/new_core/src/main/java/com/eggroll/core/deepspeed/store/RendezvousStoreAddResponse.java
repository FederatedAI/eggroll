package com.eggroll.core.deepspeed.store;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;

public class RendezvousStoreAddResponse {
    private long amount;

    public RendezvousStoreAddResponse(long amount) {
        this.amount = amount;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public static byte[] serialize(RendezvousStoreAddResponse src) {
        Deepspeed.StoreAddResponse.Builder builder = Deepspeed.StoreAddResponse.newBuilder()
                .setAmount(src.getAmount());
        return builder.build().toByteArray();
    }

    public static RendezvousStoreAddResponse deserialize(byte[] bytes) throws InvalidProtocolBufferException {
        Deepspeed.StoreAddResponse src = Deepspeed.StoreAddResponse.parseFrom(bytes);
        return new RendezvousStoreAddResponse(
                src.getAmount()
        );
    }
}