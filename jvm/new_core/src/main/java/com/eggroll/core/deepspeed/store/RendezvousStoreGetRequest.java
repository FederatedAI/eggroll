package com.eggroll.core.deepspeed.store;

import com.eggroll.core.pojo.RpcMessage;
import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

@Data
public class RendezvousStoreGetRequest implements RpcMessage {
    private String prefix;
    private byte[] key;
    private long timeout;

    @Override
    public byte[] serialize() {
        Deepspeed.StoreGetRequest.Builder builder = Deepspeed.StoreGetRequest.newBuilder()
                .setPrefix(this.getPrefix())
                .setKey(ByteString.copyFrom(JsonUtil.convertToByteArray(this.getKey())))
                .setTimeout(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(this.getTimeout())
                        .setNanos((int) this.getTimeout())
                        .build());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StoreGetRequest proto = Deepspeed.StoreGetRequest.parseFrom(data);
            this.prefix = proto.getPrefix();
            this.key = proto.getKey().toByteArray();
            this.timeout = proto.getTimeout().getSeconds();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

}