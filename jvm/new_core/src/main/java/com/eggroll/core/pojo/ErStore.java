package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ErStore implements RpcMessage {
    private ErStoreLocator storeLocator;
    private List<ErPartition> partitions;
    private Map<String, String> options;

    public ErStore(ErStoreLocator storeLocator, List<ErPartition> partitions, Map<String, String> options) {
        this.storeLocator = storeLocator;
        this.partitions = partitions;
        this.options = options;
    }

    public ErStore(ErStoreLocator storeLocator) {
        this(storeLocator, new ArrayList<>(), new ConcurrentHashMap<>());
    }

    public String toPath(String delim) {
        return storeLocator.toPath(delim);
    }

    public ErStore fork(ErStoreLocator storeLocator) {
        ErStoreLocator finalStoreLocator = storeLocator == null ? storeLocator.fork() : storeLocator;

        List<ErPartition> updatedPartitions = new ArrayList<>();
        for (int i = 0; i < partitions.size(); i++) {
            ErPartition partition = partitions.get(i);
            partition.setStoreLocator(finalStoreLocator);
        }
        return new ErStore(finalStoreLocator, updatedPartitions, options);
    }

    public ErStore fork(String postfix, String delimiter) {
        ErStoreLocator newStoreLocator = storeLocator.fork(postfix, delimiter);
        return fork(newStoreLocator);
    }

    @Override
    public byte[] serialize() {
//        Meta.Store.Builder builder = Meta.Store.newBuilder();
//        builder.setStoreLocator()
//        builder.setPartitions()
//        builder.op
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] data) {

    }
}