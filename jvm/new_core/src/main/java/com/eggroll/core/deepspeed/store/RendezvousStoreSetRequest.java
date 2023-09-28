package com.eggroll.core.deepspeed.store;

import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;

import java.util.Vector;

@Data
public class RendezvousStoreSetRequest<K> {
    private String prefix;
    private K key;
    private Vector value;

    public RendezvousStoreSetRequest(String prefix, K key, Vector value) {
        this.prefix = prefix;
        this.key = key;
        this.value = value;
    }

    public static <K, V> byte[] serialize(RendezvousStoreSetRequest<K> src) {
        Deepspeed.StoreSetRequest.Builder builder = Deepspeed.StoreSetRequest.newBuilder()
                .setPrefix(src.getPrefix())
                .setKey(ByteString.copyFrom(JsonUtil.convertToByteArray(src.getKey())));
        builder.setValue(ByteString.copyFrom(JsonUtil.convertToByteArray(src.getValue())));
        return builder.build().toByteArray();
    }

    public static RendezvousStoreSetRequest deserialize(ByteString byteString) throws InvalidProtocolBufferException {
        Deepspeed.StoreSetRequest proto = Deepspeed.StoreSetRequest.parseFrom(byteString);
        String prefix = proto.getPrefix();
        Vector<Byte> key = byteArrayToVector(proto.getKey().toByteArray());
        Vector<Byte> value = byteArrayToVector(proto.getValue().toByteArray());
        return new RendezvousStoreSetRequest(prefix, key, value);
    }

    private static Vector<Byte> byteArrayToVector(byte[] byteArray) {
        Vector<Byte> vector = new Vector<>();
        for (byte b : byteArray) {
            vector.add(b);
        }
        return vector;
    }
}