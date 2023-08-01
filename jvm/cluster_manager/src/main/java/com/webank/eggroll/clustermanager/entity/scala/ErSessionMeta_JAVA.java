package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;


import java.util.*;


public class ErSessionMeta_JAVA implements MetaRpcMessage_JAVA {
    private String id = StringConstants.EMPTY();
    private String name = StringConstants.EMPTY();
    private String status = StringConstants.EMPTY();
    private Integer totalProcCount = 0;
    private Integer activeProcCount = 0;
    private String tag = StringConstants.EMPTY();
    private List<ErProcessor_JAVA> processors = new ArrayList<>();
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

    public List<ErProcessor_JAVA> getProcessors() {
        return processors;
    }

    public void setProcessors(List<ErProcessor_JAVA> processors) {
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

}