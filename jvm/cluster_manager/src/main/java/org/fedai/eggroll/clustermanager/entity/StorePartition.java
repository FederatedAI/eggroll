package org.fedai.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

@TableName(value = "store_partition", autoResultMap = true)
public class StorePartition {
    @TableId(type = IdType.AUTO)
    private Long storePartitionId;

    private Long storeLocatorId;

    private Long nodeId;

    private Integer partitionId;

    private String status;

    private Date createdAt;

    private Date updatedAt;

    public StorePartition(Long storePartitionId, Long storeLocatorId, Long nodeId, Integer partitionId, String status, Date createdAt, Date updatedAt) {
        this.storePartitionId = storePartitionId;
        this.storeLocatorId = storeLocatorId;
        this.nodeId = nodeId;
        this.partitionId = partitionId;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public StorePartition() {
        super();
    }

    public Long getStorePartitionId() {
        return storePartitionId;
    }

    public void setStorePartitionId(Long storePartitionId) {
        this.storePartitionId = storePartitionId;
    }

    public Long getStoreLocatorId() {
        return storeLocatorId;
    }

    public void setStoreLocatorId(Long storeLocatorId) {
        this.storeLocatorId = storeLocatorId;
    }

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
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