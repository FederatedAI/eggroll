package org.fedai.eggroll.core.deepspeed.store;

import org.fedai.eggroll.core.pojo.RpcMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreGetResponse implements RpcMessage {
    private byte[] value;
    private boolean isTimeout;

    public RendezvousStoreGetResponse() {
    }

    public RendezvousStoreGetResponse(byte[] value, boolean isTimeout) {
        this.value = value;
        this.isTimeout = isTimeout;
    }

    @Override
    public byte[] serialize() {
        Deepspeed.StoreGetResponse.Builder builder = Deepspeed.StoreGetResponse.newBuilder();
        builder.setValue(ByteString.copyFrom(this.value));
        builder.setIsTimeout(this.isTimeout);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StoreGetResponse src = Deepspeed.StoreGetResponse.parseFrom(data);
            this.value = src.getValue().toByteArray();
            this.isTimeout = src.getIsTimeout();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}