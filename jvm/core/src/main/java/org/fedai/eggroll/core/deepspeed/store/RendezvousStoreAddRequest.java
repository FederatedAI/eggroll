package org.fedai.eggroll.core.deepspeed.store;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.fedai.eggroll.core.pojo.RpcMessage;
import org.fedai.eggroll.core.utils.JsonUtil;

@Data
public class RendezvousStoreAddRequest implements RpcMessage {
    private String prefix;
    private byte[] key;
    private long amount;

    @Override
    public byte[] serialize() {
        Deepspeed.StoreAddRequest.Builder builder = Deepspeed.StoreAddRequest.newBuilder()
                .setPrefix(this.getPrefix())
                .setKey(ByteString.copyFrom(JsonUtil.convertToByteArray(this.getKey())))
                .setAmount(this.getAmount());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StoreAddRequest proto = Deepspeed.StoreAddRequest.parseFrom(data);
            this.prefix = proto.getPrefix();
            this.key = proto.getKey().toByteArray();
            this.amount = proto.getAmount();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}