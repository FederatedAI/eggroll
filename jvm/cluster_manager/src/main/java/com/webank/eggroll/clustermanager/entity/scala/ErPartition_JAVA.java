package com.webank.eggroll.clustermanager.entity.scala;

import lombok.Data;

@Data
public class ErPartition_JAVA implements MetaRpcMessage_JAVA {
    private int id;
    private ErStoreLocator_JAVA storeLocator;
    private ErProcessor_JAVA processor;
    private int rankInNode;

    public ErPartition_JAVA(int id, ErStoreLocator_JAVA storeLocator, ErProcessor_JAVA processor, int rankInNode) {
        this.id = id;
        this.storeLocator = storeLocator;
        this.processor = processor;
        this.rankInNode = rankInNode;
    }

    public ErPartition_JAVA(int id) {
        this(id, null, null, -1);
    }

    public String toPath(String delim) {
        String storeLocatorPath = storeLocator != null ? storeLocator.toPath(delim) : "";
        return String.join(delim, storeLocatorPath, String.valueOf(id));
    }
}
