package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;
@TableName(value = "store_locator", autoResultMap = true)
public class StoreLocator {
    @TableId(type = IdType.AUTO)
    private Long storeLocatorId;

    private String storeType;

    private String namespace;

    private String name;

    private String path;

    private Integer totalPartitions;

    private String partitioner;

    private String serdes;

    private Integer version;

    private String status;

    private Date createdAt;

    private Date updatedAt;

    public StoreLocator(Long storeLocatorId, String storeType, String namespace, String name, String path, Integer totalPartitions, String partitioner, String serdes, Integer version, String status, Date createdAt, Date updatedAt) {
        this.storeLocatorId = storeLocatorId;
        this.storeType = storeType;
        this.namespace = namespace;
        this.name = name;
        this.path = path;
        this.totalPartitions = totalPartitions;
        this.partitioner = partitioner;
        this.serdes = serdes;
        this.version = version;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public StoreLocator() {
        super();
    }

    public Long getStoreLocatorId() {
        return storeLocatorId;
    }

    public void setStoreLocatorId(Long storeLocatorId) {
        this.storeLocatorId = storeLocatorId;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType == null ? null : storeType.trim();
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace == null ? null : namespace.trim();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path == null ? null : path.trim();
    }

    public Integer getTotalPartitions() {
        return totalPartitions;
    }

    public void setTotalPartitions(Integer totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner == null ? null : partitioner.trim();
    }

    public String getSerdes() {
        return serdes;
    }

    public void setSerdes(String serdes) {
        this.serdes = serdes == null ? null : serdes.trim();
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
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