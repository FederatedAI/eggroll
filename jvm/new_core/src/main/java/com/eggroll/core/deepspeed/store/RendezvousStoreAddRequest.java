package com.eggroll.core.deepspeed.store;

import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

import java.util.Vector;

@Data
public class RendezvousStoreAddRequest<K> {
    private String prefix;
    private K key;
    private long amount;

    public RendezvousStoreAddRequest(String prefix, K key, long amount) {
        this.prefix = prefix;
        this.key = key;
        this.amount = amount;
    }

    public static <K> byte[] serialize(RendezvousStoreAddRequest<K> src) {
        Deepspeed.StoreAddRequest.Builder builder = Deepspeed.StoreAddRequest.newBuilder()
                .setPrefix(src.getPrefix())
                .setKey(ByteString.copyFrom(JsonUtil.convertToByteArray(src.getKey())))
                .setAmount(src.getAmount());
        return builder.build().toByteArray();
    }

    public static <K> RendezvousStoreAddRequest<K> deserialize(ByteString byteString) throws InvalidProtocolBufferException {
        Deepspeed.StoreAddRequest proto = Deepspeed.StoreAddRequest.parseFrom(byteString);
        String prefix = proto.getPrefix();
        Vector<Byte> key = byteArrayToVector(proto.getKey().toByteArray());
        long amount = proto.getAmount();
        return new RendezvousStoreAddRequest(prefix, key, amount);
    }

    private static Vector<Byte> byteArrayToVector(byte[] byteArray) {
        Vector<Byte> vector = new Vector<>();
        for (byte b : byteArray) {
            vector.add(b);
        }
        return vector;
    }
}