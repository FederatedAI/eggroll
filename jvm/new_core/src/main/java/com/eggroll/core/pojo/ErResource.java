package com.eggroll.core.pojo;


import com.eggroll.core.config.Dict;

import java.util.ArrayList;
import java.util.List;

public class ErResource   {
    private long resourceId= -1;
    private String resourceType= Dict.EMPTY;
    private long serverNodeId= 0;
    private long total= -1;
    private long used= -1;
    private long allocated= -1;
    private long preAllocated= -1;
    private String extention = null;
    private String status = Dict.AVAILABLE;
    private List<String> extentionCache = new ArrayList<>();


    public long getResourceId() {
        return resourceId;
    }

    public void setResourceId(long resourceId) {
        this.resourceId = resourceId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public long getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(long serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getUsed() {
        return used;
    }

    public void setUsed(long used) {
        this.used = used;
    }

    public long getAllocated() {
        return allocated;
    }

    public void setAllocated(long allocated) {
        this.allocated = allocated;
    }

    public long getPreAllocated() {
        return preAllocated;
    }

    public void setPreAllocated(long preAllocated) {
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

    public long getUnAllocatedResource() {
        long remain = total;
        if (allocated > 0) {
            remain = remain - allocated;
        }
        if (preAllocated > 0) {
            remain = remain - preAllocated;
        }
        return remain;
    }
}