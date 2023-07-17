package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.NetworkingRpcMessage;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ErServerCluster_JAVA implements NetworkingRpcMessage {
    private long id;
    private String name;
    private List<ErServerNode_JAVA> serverNodes;
    private String tag;

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