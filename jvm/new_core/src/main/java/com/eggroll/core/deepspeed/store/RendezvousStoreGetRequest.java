package com.eggroll.core.deepspeed.store;

import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

import java.util.Vector;

@Data
public class RendezvousStoreGetRequest<K> {
    private String prefix;
    private K key;
    private long timeout;

    public RendezvousStoreGetRequest(String prefix, K key, long timeout) {
        this.prefix = prefix;
        this.key = key;
        this.timeout = timeout;
    }

    public String getPrefix() {
        return prefix;
    }

    public K getKey() {
        return key;
    }

    public long getTimeout() {
        return timeout;
    }

    public static <K> byte[] serialize(RendezvousStoreGetRequest<K> src) {
        Deepspeed.StoreGetRequest.Builder builder = Deepspeed.StoreGetRequest.newBuilder()
                .setPrefix(src.getPrefix())
                .setKey(ByteString.copyFrom(JsonUtil.convertToByteArray(src.getKey())))
                .setTimeout(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(src.getTimeout())
                        .setNanos((int)src.getTimeout())
                        .build());
        return builder.build().toByteArray();
    }

    public static <K> RendezvousStoreGetRequest<K> deserialize(ByteString byteString) throws InvalidProtocolBufferException {
        Deepspeed.StoreGetRequest proto = Deepspeed.StoreGetRequest.parseFrom(byteString);
        String prefix = proto.getPrefix();
        Vector<Byte> key = byteArrayToVector(proto.getKey().toByteArray());
        return new RendezvousStoreGetRequest(prefix, key, proto.getTimeout().getNanos());
    }

    private static Vector<Byte> byteArrayToVector(byte[] byteArray) {
        Vector<Byte> vector = new Vector<>();
        for (byte b : byteArray) {
            vector.add(b);
        }
        return vector;
    }
}