package com.eggroll.core.pojo;


import com.eggroll.core.config.Dict;

import java.util.ArrayList;
import java.util.List;

public class ErServerCluster implements  RpcMessage {
    private long id;
    private String name;
    private List<ErServerNode> serverNodes;
    private String tag;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ErServerNode> getServerNodes() {
        return serverNodes;
    }

    public void setServerNodes(List<ErServerNode> serverNodes) {
        this.serverNodes = serverNodes;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public ErServerCluster() {
        this.id = -1;
        this.name = Dict.EMPTY;
        this.serverNodes = new ArrayList<>();
        this.tag = Dict.EMPTY;
    }

    public ErServerCluster(long id, List<ErServerNode> serverNodes, String tag) {
        this.id = id;
        this.name = Dict.EMPTY;
        this.serverNodes = serverNodes;
        this.tag = tag;
    }

    @Override
    public String toString() {//TODO List的输出
        return "<ErServerCluster(id=" + id + ", name=" + name +
                ", serverNodes=" + serverNodes.toString() + ", tag=" + tag +
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