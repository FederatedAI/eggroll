package com.webank.eggroll.clustermanager.entity.scala;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ErStore_JAVA implements MetaRpcMessage_JAVA {
    private ErStoreLocator_JAVA storeLocator;
    private List<ErPartition_JAVA> partitions;
    private Map<String, String> options;

    public ErStore_JAVA(ErStoreLocator_JAVA storeLocator, List<ErPartition_JAVA> partitions, Map<String, String> options) {
        this.storeLocator = storeLocator;
        this.partitions = partitions;
        this.options = options;
    }

    public ErStore_JAVA(ErStoreLocator_JAVA storeLocator) {
        this(storeLocator, new ArrayList<>(), new ConcurrentHashMap<>());
    }

    public String toPath(String delim) {
        return storeLocator.toPath(delim);
    }

    public ErStore_JAVA fork(ErStoreLocator_JAVA storeLocator) {
        ErStoreLocator_JAVA finalStoreLocator = storeLocator == null ? storeLocator.fork() : storeLocator;

        List<ErPartition_JAVA> updatedPartitions = new ArrayList<>();
        for (int i = 0; i < partitions.size(); i++) {
            ErPartition_JAVA partition = partitions.get(i);
            partition.setStoreLocator(finalStoreLocator);
        }
        return new ErStore_JAVA(finalStoreLocator, updatedPartitions, options);
    }

    public ErStore_JAVA fork(String postfix, String delimiter) {
        ErStoreLocator_JAVA newStoreLocator = storeLocator.fork(postfix, delimiter);
        return fork(newStoreLocator);
    }
}