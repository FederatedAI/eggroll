package com.eggroll.core.deepspeed.store;

import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;

public class RendezvousStoreGetResponse<V> {
    private V value;
    private boolean isTimeout;

    public RendezvousStoreGetResponse(V value, boolean isTimeout) {
        this.value = value;
        this.isTimeout = isTimeout;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public boolean isTimeout() {
        return isTimeout;
    }

    public void setTimeout(boolean isTimeout) {
        this.isTimeout = isTimeout;
    }

    public static <V> byte[] serialize(RendezvousStoreGetResponse<V> src) {
        Deepspeed.StoreGetResponse.Builder builder = Deepspeed.StoreGetResponse.newBuilder();
        builder.setValue(ByteString.copyFrom(JsonUtil.convertToByteArray(src.getValue())));
        builder.setIsTimeout(src.isTimeout);
        return builder.build().toByteArray();
    }

    public static <V> RendezvousStoreGetResponse<V> deserialize(byte[] bytes) throws InvalidProtocolBufferException {
        Deepspeed.StoreGetResponse src = Deepspeed.StoreGetResponse.parseFrom(bytes);
        return new RendezvousStoreGetResponse<>(
                (V) src.getValue(),
                src.getIsTimeout()
        );
    }

}