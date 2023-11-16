package org.fedai.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

@TableName(value = "store_locator", autoResultMap = true)
@Data
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
    }

}