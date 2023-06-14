package com.webank.eggroll.core.entity;

public class SessionRanks {
    private Long containerId;

    private String sessionId;

    private Integer serverNodeId;

    private Integer globalRank;

    private Integer localRank;

    public SessionRanks(Long containerId, String sessionId, Integer serverNodeId, Integer globalRank, Integer localRank) {
        this.containerId = containerId;
        this.sessionId = sessionId;
        this.serverNodeId = serverNodeId;
        this.globalRank = globalRank;
        this.localRank = localRank;
    }

    public SessionRanks() {
        super();
    }

    public Long getContainerId() {
        return containerId;
    }

    public void setContainerId(Long containerId) {
        this.containerId = containerId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId == null ? null : sessionId.trim();
    }

    public Integer getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(Integer serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public Integer getGlobalRank() {
        return globalRank;
    }

    public void setGlobalRank(Integer globalRank) {
        this.globalRank = globalRank;
    }

    public Integer getLocalRank() {
        return localRank;
    }

    public void setLocalRank(Integer localRank) {
        this.localRank = localRank;
    }
}