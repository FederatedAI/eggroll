package com.eggroll.core.pojo;


public class ErPartition implements MetaRpcMessage {
    private int id;
    private ErStoreLocator storeLocator;
    private ErProcessor processor;
    private int rankInNode;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ErStoreLocator getStoreLocator() {
        return storeLocator;
    }

    public void setStoreLocator(ErStoreLocator storeLocator) {
        this.storeLocator = storeLocator;
    }

    public ErProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(ErProcessor processor) {
        this.processor = processor;
    }

    public int getRankInNode() {
        return rankInNode;
    }

    public void setRankInNode(int rankInNode) {
        this.rankInNode = rankInNode;
    }

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
