package org.fedai.eggroll.core.deepspeed.store;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.fedai.eggroll.core.pojo.RpcMessage;

@Data
public class RendezvousStoreDestroyResponse implements RpcMessage {
    private boolean success;

    public static byte[] serialize(RendezvousStoreDestroyResponse src) {
        Deepspeed.StoreDestroyResponse.Builder builder = Deepspeed.StoreDestroyResponse.newBuilder()
                .setSuccess(src.isSuccess());
        return builder.build().toByteArray();
    }

    @Override
    public byte[] serialize() {
        Deepspeed.StoreDestroyResponse.Builder builder = Deepspeed.StoreDestroyResponse.newBuilder()
                .setSuccess(this.isSuccess());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StoreDestroyResponse src = Deepspeed.StoreDestroyResponse.parseFrom(data);
            this.success = src.getSuccess();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}