package com.eggroll.core.pojo;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.constant.StringConstants;


import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErSessionMeta  {


    private String id = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private SessionStatus status = null;
    private Integer totalProcCount = 0;
    private Integer activeProcCount = 0;
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

    public SessionStatus getStatus() {
        return status;
    }

    public void setStatus(SessionStatus status) {
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



}