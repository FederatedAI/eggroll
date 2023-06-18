package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;
@TableName(value = "session_processor", autoResultMap = true)
public class SessionProcessor {
    private Long processorId;

    private String sessionId;

    private Integer serverNodeId;

    private String processorType;

    private String status;

    private String tag="";

    private String commandEndpoint="";

    private String transferEndpoint="";

    private String processorOption="";

    private Integer pid;

    private Date createdAt;

    private Date updatedAt;

    public SessionProcessor(Long processorId, String sessionId, Integer serverNodeId, String processorType, String status, String tag, String commandEndpoint, String transferEndpoint, String processorOption, Integer pid, Date createdAt, Date updatedAt) {
        this.processorId = processorId;
        this.sessionId = sessionId;
        this.serverNodeId = serverNodeId;
        this.processorType = processorType;
        this.status = status;
        this.tag = tag;
        this.commandEndpoint = commandEndpoint;
        this.transferEndpoint = transferEndpoint;
        this.processorOption = processorOption;
        this.pid = pid;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public SessionProcessor() {
        super();
    }

    public Long getProcessorId() {
        return processorId;
    }

    public void setProcessorId(Long processorId) {
        this.processorId = processorId;
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

    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType == null ? null : processorType.trim();
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status == null ? null : status.trim();
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag == null ? null : tag.trim();
    }

    public String getCommandEndpoint() {
        return commandEndpoint;
    }

    public void setCommandEndpoint(String commandEndpoint) {
        this.commandEndpoint = commandEndpoint == null ? null : commandEndpoint.trim();
    }

    public String getTransferEndpoint() {
        return transferEndpoint;
    }

    public void setTransferEndpoint(String transferEndpoint) {
        this.transferEndpoint = transferEndpoint == null ? null : transferEndpoint.trim();
    }

    public String getProcessorOption() {
        return processorOption;
    }

    public void setProcessorOption(String processorOption) {
        this.processorOption = processorOption == null ? null : processorOption.trim();
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
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