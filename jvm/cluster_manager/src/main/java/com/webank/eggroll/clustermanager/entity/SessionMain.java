package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.pojo.ErSessionMeta;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
@TableName(value = "session_main", autoResultMap = true)
public class SessionMain {
    @TableId(type = IdType.INPUT)
    private String sessionId;

    private String name;

    private String status;

    private String tag;

    private Integer totalProcCount;

    private Integer activeProcCount;

    private Date createdAt;

    private Date updatedAt;

    public SessionMain(String sessionId, String name, String status, String tag, Integer totalProcCount, Integer activeProcCount, Date createdAt, Date updatedAt) {
        this.sessionId = sessionId;
        this.name = name;
        this.status = status;
        this.tag = tag;
        this.totalProcCount = totalProcCount;
        this.activeProcCount = activeProcCount;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public SessionMain() {
        super();
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId == null ? null : sessionId.trim();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
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

    public ErSessionMeta  toErSessionMeta(){
        ErSessionMeta  result = new ErSessionMeta();
        result.setId(this.sessionId);
        result.setName(this.name);
        result.setTag(this.tag);
        result.setCreateTime(createdAt);
        result.setUpdateTime(updatedAt);
        if(StringUtils.isNotEmpty(status))
            result.setStatus(SessionStatus.valueOf(status));
        result.setTotalProcCount(totalProcCount);
        result.setActiveProcCount(activeProcCount);
        return  result;

    }
}