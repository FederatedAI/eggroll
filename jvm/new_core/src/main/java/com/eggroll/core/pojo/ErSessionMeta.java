package com.eggroll.core.pojo;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import org.apache.commons.lang3.StringUtils;


import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErSessionMeta implements RpcMessage {


    private String id = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String  status = null;
    private Integer totalProcCount = null;
    private Integer activeProcCount = null;
    private String tag = StringConstants.EMPTY;
    private List<ErProcessor> processors = new ArrayList<>();
    private Date createTime = null;
    private Date updateTime = null;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getTotalProcCount() {
        return totalProcCount;
    }

    public void setTotalProcCount(Integer totalProcCount) {
        this.totalProcCount = totalProcCount;
    }

    public Integer getActiveProcCount() {
        return activeProcCount;
    }

    public void setActiveProcCount(Integer activeProcCount) {
        this.activeProcCount = activeProcCount;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public List<ErProcessor> getProcessors() {
        return processors;
    }

    public void setProcessors(List<ErProcessor> processors) {
        this.processors = processors;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    private Map<String, String> options = new HashMap<>();


    @Override
    public byte[] serialize() {
        Meta.SessionMeta.Builder  builder = Meta.SessionMeta.newBuilder();
        builder.setId(this.id);
        builder.setName(this.name);
        if(status!=null){
            builder.setStatus(status);
        }
        for(ErProcessor erProcessor:processors){
            builder.addProcessors(erProcessor.toProto());
        }
        builder.setTag(this.tag);
        return builder.build().toByteArray();

    }

    @Override
    public String toString() {
        return JsonUtil.object2Json(this);
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.SessionMeta  sessionMeta =  Meta.SessionMeta.parseFrom(data);
            this.status = sessionMeta.getStatus();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    public   boolean isOverState(){
        if (StringUtils.equalsAny(this.status, SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name())){
            return true;
        }else
        {
            return false;
        }

    }

}