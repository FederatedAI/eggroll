package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;

import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ErServerNode_JAVA implements NetworkingRpcMessage_JAVA {
    private long id;
    private String name;
    private long clusterId;
    private Entity_Collects_JAVA.ErEndpoint endpoint;
    private String nodeType;
    private String status;
    private Timestamp lastHeartBeat;
    private List<ErResource_JAVA> resources;

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

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public Entity_Collects_JAVA.ErEndpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Entity_Collects_JAVA.ErEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Timestamp getLastHeartBeat() {
        return lastHeartBeat;
    }

    public void setLastHeartBeat(Timestamp lastHeartBeat) {
        this.lastHeartBeat = lastHeartBeat;
    }

    public List<ErResource_JAVA> getResources() {
        return resources;
    }

    public void setResources(List<ErResource_JAVA> resources) {
        this.resources = resources;
    }

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