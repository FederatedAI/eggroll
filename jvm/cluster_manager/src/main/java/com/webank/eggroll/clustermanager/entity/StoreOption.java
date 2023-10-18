package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;
@TableName(value = "store_option", autoResultMap = true)
public class StoreOption {
    @TableId(type = IdType.AUTO)
    private Long storeOptionId;

    private Long storeLocatorId;

    private String name;

    private String data;

    private Date createdAt;

    private Date updatedAt;

    public StoreOption(Long storeOptionId, Long storeLocatorId, String name, String data, Date createdAt, Date updatedAt) {
        this.storeOptionId = storeOptionId;
        this.storeLocatorId = storeLocatorId;
        this.name = name;
        this.data = data;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public StoreOption() {
        super();
    }

    public Long getStoreOptionId() {
        return storeOptionId;
    }

    public void setStoreOptionId(Long storeOptionId) {
        this.storeOptionId = storeOptionId;
    }

    public Long getStoreLocatorId() {
        return storeLocatorId;
    }

    public void setStoreLocatorId(Long storeLocatorId) {
        this.storeLocatorId = storeLocatorId;
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