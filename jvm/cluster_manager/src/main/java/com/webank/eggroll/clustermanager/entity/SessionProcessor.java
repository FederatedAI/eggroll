package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.utils.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.Map;

@TableName(value = "session_processor", autoResultMap = true)
@Data
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
        this.serverNodeId = erProcessor.getServerNodeId().intValue();
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


    public ErProcessor toErProcessor() {
        ErProcessor result = new ErProcessor();
        result.setId(this.processorId);
        result.setSessionId(this.sessionId);
        if (this.serverNodeId != null) {
            result.setServerNodeId(this.serverNodeId.longValue());
        }
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
            result.setOptions(JsonUtil.json2Object(this.processorOption, new TypeReference<Map<String, String>>() {
            }));
        }
        result.setPid(this.pid);
        result.setCreatedAt(this.createdAt);
        result.setUpdatedAt(this.updatedAt);
        return result;
    }
}