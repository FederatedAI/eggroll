package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.NetworkingRpcMessage;
import lombok.Data;

import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
public class ErServerNode_JAVA implements NetworkingRpcMessage {
    private long id;
    private String name;
    private long clusterId;
    private Entity_Collects_JAVA.ErEndpoint endpoint;
    private String nodeType;
    private String status;
    private Timestamp lastHeartBeat;
    private List<ErResource_JAVA> resources;

    public ErServerNode_JAVA() {
        this.id = -1;
        this.name = StringConstants.EMPTY();
        this.clusterId = 0;
        this.endpoint = new Entity_Collects_JAVA.ErEndpoint(StringConstants.EMPTY(), -1);
        this.nodeType = StringConstants.EMPTY();
        this.status = StringConstants.EMPTY();
        this.lastHeartBeat = null;
        this.resources = new ArrayList<>();
    }

    public ErServerNode_JAVA(long id, String name, long clusterId, Entity_Collects_JAVA.ErEndpoint endpoint,
                        String nodeType, String status, Timestamp lastHeartBeat,
                        List<ErResource_JAVA> resources) {
        this.id = id;
        this.name = name;
        this.clusterId = clusterId;
        this.endpoint = endpoint;
        this.nodeType = nodeType;
        this.status = status;
        this.lastHeartBeat = lastHeartBeat;
        this.resources = resources;
    }

    public ErServerNode_JAVA(String nodeType, String status) {
        this.id = -1;
        this.name = StringConstants.EMPTY();
        this.clusterId = 0;
        this.endpoint = new Entity_Collects_JAVA.ErEndpoint(StringConstants.EMPTY(), -1);
        this.nodeType = nodeType;
        this.status = status;
        this.lastHeartBeat = null;
        this.resources = new ArrayList<>();
    }

    @Override
    public String toString() {//TODO List的输出
        return "<ErServerNode(id=" + id + ", name=" + name +
                ", clusterId=" + clusterId + ", endpoint=" + endpoint +
                ", nodeType=" + nodeType + ", status=" + status +
                ", lastHeartBeat=" + lastHeartBeat +
                ", resources=" + resources.toString() +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }
}