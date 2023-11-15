package org.fedai.eggroll.core.deepspeed.store;

import org.fedai.eggroll.core.pojo.RpcMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreAddResponse implements RpcMessage {
    private long amount;

    public RendezvousStoreAddResponse(long amount) {
        this.amount = amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    @Override
    public byte[] serialize() {
        Deepspeed.StoreAddResponse.Builder builder = Deepspeed.StoreAddResponse.newBuilder()
                .setAmount(this.getAmount());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StoreAddResponse src = Deepspeed.StoreAddResponse.parseFrom(data);
            this.amount = src.getAmount();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}