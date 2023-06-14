package com.webank.eggroll.core.entity;

import java.util.Date;

public class ServerNode {
    private Long serverNodeId;

    private String name;

    private Long serverClusterId;

    private String host;

    private Integer port;

    private String nodeType;

    private String status;

    private Date lastHeartbeatAt;

    private Date createdAt;

    private Date updatedAt;

    public ServerNode(Long serverNodeId, String name, Long serverClusterId, String host, Integer port, String nodeType, String status, Date lastHeartbeatAt, Date createdAt, Date updatedAt) {
        this.serverNodeId = serverNodeId;
        this.name = name;
        this.serverClusterId = serverClusterId;
        this.host = host;
        this.port = port;
        this.nodeType = nodeType;
        this.status = status;
        this.lastHeartbeatAt = lastHeartbeatAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public ServerNode() {
        super();
    }

    public Long getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(Long serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    public Long getServerClusterId() {
        return serverClusterId;
    }

    public void setServerClusterId(Long serverClusterId) {
        this.serverClusterId = serverClusterId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host == null ? null : host.trim();
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType == null ? null : nodeType.trim();
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status == null ? null : status.trim();
    }

    public Date getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    public void setLastHeartbeatAt(Date lastHeartbeatAt) {
        this.lastHeartbeatAt = lastHeartbeatAt;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }
}