package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.utils.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.Map;

@TableName(value = "session_processor", autoResultMap = true)
public class SessionProcessor {
    @TableId(type = IdType.AUTO)
    private Long processorId;

    private String sessionId;

    private Integer serverNodeId;

    private String processorType;

    private String status;

    private String tag = "";

    private String commandEndpoint = "";

    private String transferEndpoint = "";

    private String processorOption = "";

    private Integer pid;

    private Date createdAt;

    private Date updatedAt;

    public SessionProcessor(ErProcessor erProcessor) {
        this.processorId = erProcessor.getId();
        this.sessionId = erProcessor.getSessionId();
        this.serverNodeId =  erProcessor.getServerNodeId().intValue();
        this.processorType = erProcessor.getProcessorType();
        this.status = erProcessor.getStatus();
        this.tag = erProcessor.getTag();

        this.commandEndpoint = erProcessor.getCommandEndpoint() != null ? erProcessor.getCommandEndpoint().toString() : null;
        this.transferEndpoint = erProcessor.getTransferEndpoint() != null ? erProcessor.getTransferEndpoint().toString() : null;
        this.processorOption = erProcessor.getOptions() != null ? JsonUtil.object2Json(erProcessor.getOptions()) : null;

        this.pid = erProcessor.getPid();
        this.createdAt = erProcessor.getCreatedAt();
        this.updatedAt = erProcessor.getUpdatedAt();
    }

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

    public ErProcessor toErProcessor() {

//        this.processorId = processorId;
//        this.sessionId = sessionId;
//        this.serverNodeId = serverNodeId;
//        this.processorType = processorType;
//        this.status = status;
//        this.tag = tag;
//        this.commandEndpoint = commandEndpoint;
//        this.transferEndpoint = transferEndpoint;
//        this.processorOption = processorOption;
//        this.pid = pid;
//        this.createdAt = createdAt;
//        this.updatedAt = updatedAt;
        ErProcessor result = new ErProcessor();
        result.setId(this.processorId);
        result.setSessionId(this.sessionId);
        result.setServerNodeId(this.serverNodeId.longValue());
        result.setProcessorType(this.processorType);
        result.setStatus(this.status);
        result.setTag(this.tag);

        if (StringUtils.isNotEmpty(this.commandEndpoint)) {
            ErEndpoint ep = new ErEndpoint(this.commandEndpoint);
            result.setCommandEndpoint(ep);
        }

        if (StringUtils.isNotEmpty(this.transferEndpoint)) {
            ErEndpoint ep = new ErEndpoint(this.transferEndpoint);
            result.setTransferEndpoint(ep);
        }
        if (StringUtils.isNotEmpty(this.processorOption)) {
            result.setOptions(JsonUtil.json2Object(this.processorOption, Map.class));
        }
        result.setPid(this.pid);
        result.setCreatedAt(this.createdAt);
        result.setUpdatedAt(this.updatedAt);
        return result;
    }
}