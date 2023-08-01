package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.NetworkingRpcMessage;


import java.util.ArrayList;
import java.util.List;


public class ErServerCluster_JAVA {
    private long id;
    private String name;
    private List<ErServerNode_JAVA> serverNodes;
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

    public List<ErServerNode_JAVA> getServerNodes() {
        return serverNodes;
    }

    public void setServerNodes(List<ErServerNode_JAVA> serverNodes) {
        this.serverNodes = serverNodes;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public ErServerCluster_JAVA() {
        this.id = -1;
        this.name = StringConstants.EMPTY();
        this.serverNodes = new ArrayList<>();
        this.tag = StringConstants.EMPTY();
    }

    public ErServerCluster_JAVA(long id, List<ErServerNode_JAVA> serverNodes, String tag) {
        this.id = id;
        this.name = StringConstants.EMPTY();
        this.serverNodes = serverNodes;
        this.tag = tag;
    }

    @Override
    public String toString() {//TODO List的输出
        return "<ErServerCluster(id=" + id + ", name=" + name +
                ", serverNodes=" + serverNodes.toString() + ", tag=" + tag +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }
}