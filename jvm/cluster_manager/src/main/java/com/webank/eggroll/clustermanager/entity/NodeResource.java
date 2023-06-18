package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;
@TableName(value = "node_resource", autoResultMap = true)
public class NodeResource {
    @TableId(type = IdType.AUTO)
    private Long resourceId;

    private Long serverNodeId;

    private String resourceType;

    private Long total;

    private Long used;

    private Long preAllocated;

    private Long allocated;

    private String extention;

    private String status;

    private Date createdAt;

    private Date updatedAt;

    public NodeResource(Long resourceId, Long serverNodeId, String resourceType, Long total, Long used, Long preAllocated, Long allocated, String extention, String status, Date createdAt, Date updatedAt) {
        this.resourceId = resourceId;
        this.serverNodeId = serverNodeId;
        this.resourceType = resourceType;
        this.total = total;
        this.used = used;
        this.preAllocated = preAllocated;
        this.allocated = allocated;
        this.extention = extention;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public NodeResource() {
        super();
    }

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public Long getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(Long serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType == null ? null : resourceType.trim();
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Long getUsed() {
        return used;
    }

    public void setUsed(Long used) {
        this.used = used;
    }

    public Long getPreAllocated() {
        return preAllocated;
    }

    public void setPreAllocated(Long preAllocated) {
        this.preAllocated = preAllocated;
    }

    public Long getAllocated() {
        return allocated;
    }

    public void setAllocated(Long allocated) {
        this.allocated = allocated;
    }

    public String getExtention() {
        return extention;
    }

    public void setExtention(String extention) {
        this.extention = extention == null ? null : extention.trim();
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status == null ? null : status.trim();
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