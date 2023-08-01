package com.webank.eggroll.clustermanager.entity.scala;




public class ErPartition_JAVA implements MetaRpcMessage_JAVA {
    private int id;
    private ErStoreLocator_JAVA storeLocator;
    private ErProcessor_JAVA processor;
    private int rankInNode;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ErStoreLocator_JAVA getStoreLocator() {
        return storeLocator;
    }

    public void setStoreLocator(ErStoreLocator_JAVA storeLocator) {
        this.storeLocator = storeLocator;
    }

    public ErProcessor_JAVA getProcessor() {
        return processor;
    }

    public void setProcessor(ErProcessor_JAVA processor) {
        this.processor = processor;
    }

    public int getRankInNode() {
        return rankInNode;
    }

    public void setRankInNode(int rankInNode) {
        this.rankInNode = rankInNode;
    }

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
