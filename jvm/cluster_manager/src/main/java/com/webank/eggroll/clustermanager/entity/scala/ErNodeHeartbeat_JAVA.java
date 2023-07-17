package com.webank.eggroll.clustermanager.entity.scala;

import lombok.Data;

@Data
public class ErNodeHeartbeat_JAVA implements NetworkingRpcMessage_JAVA {
    private long id;
    private ErServerNode_JAVA node;

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