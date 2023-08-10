package com.eggroll.core.pojo;


import lombok.Data;

@Data
public class ErPartition implements MetaRpcMessage {
    private int id;
    private ErStoreLocator storeLocator;
    private ErProcessor processor;
    private int rankInNode;


    public ErPartition(int id, ErStoreLocator storeLocator, ErProcessor processor, int rankInNode) {
        this.id = id;
        this.storeLocator = storeLocator;
        this.processor = processor;
        this.rankInNode = rankInNode;
    }

    public ErPartition(int id) {
        this(id, null, null, -1);
    }

    public String toPath(String delim) {
        String storeLocatorPath = storeLocator != null ? storeLocator.toPath(delim) : "";
        return String.join(delim, storeLocatorPath, String.valueOf(id));
    }
}
