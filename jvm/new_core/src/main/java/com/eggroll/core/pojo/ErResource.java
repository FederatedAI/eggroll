package com.eggroll.core.pojo;


import com.eggroll.core.config.Dict;

import java.util.ArrayList;
import java.util.List;

public class ErResource   implements RpcMessage{
    private Long resourceId= -1L;
    private String resourceType= Dict.EMPTY;
    private Long serverNodeId= 0L;
    private Long total= -1L;
    private Long used= -1L;
    private Long allocated= -1L;
    private Long preAllocated= -1L;
    private String extention = null;
    private String status = Dict.AVAILABLE;
    private List<String> extentionCache = new ArrayList<>();


    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public Long getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(Long serverNodeId) {
        this.serverNodeId = serverNodeId;
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

    public Long getAllocated() {
        return allocated;
    }

    public void setAllocated(Long allocated) {
        this.allocated = allocated;
    }

    public Long getPreAllocated() {
        return preAllocated;
    }

    public void setPreAllocated(Long preAllocated) {
        this.preAllocated = preAllocated;
    }

    public String getExtention() {
        return extention;
    }

    public void setExtention(String extention) {
        this.extention = extention;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<String> getExtentionCache() {
        return extentionCache;
    }

    public void setExtentionCache(List<String> extentionCache) {
        this.extentionCache = extentionCache;
    }

    public Long getUnAllocatedResource() {
        Long remain = total;
        if (allocated > 0) {
            remain = remain - allocated;
        }
        if (preAllocated > 0) {
            remain = remain - preAllocated;
        }
        return remain;
    }

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] data) {

    }
}