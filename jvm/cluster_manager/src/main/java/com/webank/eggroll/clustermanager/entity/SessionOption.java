package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;
@TableName(value = "session_option", autoResultMap = true)
public class SessionOption {
    @TableId
    private Long sessionOptionId;

    private String sessionId;

    private String name;

    private String data;

    private Date createdAt;

    private Date updatedAt;

    public SessionOption(Long sessionOptionId, String sessionId, String name, String data, Date createdAt, Date updatedAt) {
        this.sessionOptionId = sessionOptionId;
        this.sessionId = sessionId;
        this.name = name;
        this.data = data;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public SessionOption() {
        super();
    }

    public Long getSessionOptionId() {
        return sessionOptionId;
    }

    public void setSessionOptionId(Long sessionOptionId) {
        this.sessionOptionId = sessionOptionId;
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

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data == null ? null : data.trim();
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