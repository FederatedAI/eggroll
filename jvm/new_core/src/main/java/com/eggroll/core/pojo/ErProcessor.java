package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;

import com.webank.eggroll.core.meta.Meta;


import java.util.*;


public class ErProcessor {
    private long id = -1;
    private String sessionId = StringConstants.EMPTY;
    private long serverNodeId = -1;
    private String name = StringConstants.EMPTY;
    private String processorType = StringConstants.EMPTY;
    private String status = StringConstants.EMPTY;
    private ErEndpoint commandEndpoint = null;
    private ErEndpoint transferEndpoint = null;
    private int pid = -1;
    private Map<String, String> options = new HashMap<>();
    private String tag = StringConstants.EMPTY;
    private List<ErResource> resources = new ArrayList<>();
    private Date createdAt = null;
    private Date updatedAt = null;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(long serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public ErEndpoint getCommandEndpoint() {
        return commandEndpoint;
    }

    public void setCommandEndpoint(ErEndpoint commandEndpoint) {
        this.commandEndpoint = commandEndpoint;
    }

    public ErEndpoint getTransferEndpoint() {
        return transferEndpoint;
    }

    public void setTransferEndpoint(ErEndpoint transferEndpoint) {
        this.transferEndpoint = transferEndpoint;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public List<ErResource> getResources() {
        return resources;
    }

    public void setResources(List<ErResource> resources) {
        this.resources = resources;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (resources != null) {
            for (ErResource resource : resources) {
                sb.append(resource.toString());
            }
        }
        //TODO List的输出
        return "<ErProcessor(id=" + id + ", sessionId=" + sessionId +
                ", serverNodeId=" + serverNodeId + ", name=" + name +
                ", processorType=" + processorType + ", status=" + status +
                ", commandEndpoint=" + commandEndpoint + ", transferEndpoint=" + transferEndpoint +
                ", createdAt=" + createdAt + ", updatedAt=" + updatedAt +
                ", pid=" + pid + ", options=" + options + ", tag=" + tag +
                ") at " + Integer.toHexString(hashCode()) + " resources " + sb.toString() + ">";
    }

    public Meta.Processor toProto() {
        Meta.Processor.Builder builder = Meta.Processor.newBuilder()
                .setId(this.getId())
                .setServerNodeId(this.getServerNodeId())
                .setName(this.getName())
                .setProcessorType(this.getProcessorType())
                .setStatus(this.getStatus())
                .setCommandEndpoint(this.getCommandEndpoint() != null ? this.getCommandEndpoint().toProto() : Meta.Endpoint.getDefaultInstance())
                .setTransferEndpoint(this.getTransferEndpoint() != null ? this.getTransferEndpoint().toProto() : Meta.Endpoint.getDefaultInstance())
                .setPid(this.getPid())
                .putAllOptions(this.getOptions())
                .setTag(this.getTag());

        return builder.build();
    }
}