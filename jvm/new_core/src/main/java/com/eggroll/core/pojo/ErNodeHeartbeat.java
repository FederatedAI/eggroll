package com.eggroll.core.pojo;


public class ErNodeHeartbeat implements NetworkingRpcMessage {
    private long id;
    private ErServerNode node;

    public ErNodeHeartbeat() {
        this.id = -1;
        this.node = null;
    }

    public ErNodeHeartbeat(long id, ErServerNode node) {
        this.id = id;
        this.node = node;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ErServerNode getNode() {
        return node;
    }

    public void setNode(ErServerNode node) {
        this.node = node;
    }

    @Override
    public String toString() {
        return "<ErNodeHeartbeat(id=" + id + ", node=" + node +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] data) {

    }
}