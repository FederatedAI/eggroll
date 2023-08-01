package com.webank.eggroll.clustermanager.entity.scala;




public class ErNodeHeartbeat_JAVA implements NetworkingRpcMessage_JAVA {
    private long id;
    private ErServerNode_JAVA node;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ErServerNode_JAVA getNode() {
        return node;
    }

    public void setNode(ErServerNode_JAVA node) {
        this.node = node;
    }

    public ErNodeHeartbeat_JAVA() {
        this.id = -1;
        this.node = null;
    }

    public ErNodeHeartbeat_JAVA(long id, ErServerNode_JAVA node) {
        this.id = id;
        this.node = node;
    }

    @Override
    public String toString() {
        return "<ErNodeHeartbeat(id=" + id + ", node=" + node +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }
}