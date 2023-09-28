package com.eggroll.core.deepspeed.store;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;

public class RendezvousStoreDestroyResponse {
    private boolean success;

    public RendezvousStoreDestroyResponse(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public static byte[] serialize(RendezvousStoreDestroyResponse src) {
        Deepspeed.StoreDestroyResponse.Builder builder = Deepspeed.StoreDestroyResponse.newBuilder()
                .setSuccess(src.isSuccess());
        return builder.build().toByteArray();
    }

    public static RendezvousStoreDestroyResponse deserialize(byte[] bytes) throws InvalidProtocolBufferException {
        Deepspeed.StoreDestroyResponse src = Deepspeed.StoreDestroyResponse.parseFrom(bytes);
        return new RendezvousStoreDestroyResponse(
                src.getSuccess()
        );
    }
}