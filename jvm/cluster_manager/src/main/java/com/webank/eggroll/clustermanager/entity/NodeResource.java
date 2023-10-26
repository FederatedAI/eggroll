package com.webank.eggroll.clustermanager.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.eggroll.core.pojo.ErResource;
import lombok.Data;

import java.util.Date;

@Data
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

    public ErResource toErResource() {
        ErResource erResource = new ErResource();
        erResource.setResourceId(this.resourceId);
        erResource.setResourceType(this.resourceType);
        erResource.setTotal(this.total);
        erResource.setUsed(this.used);
        erResource.setPreAllocated(this.preAllocated);
        erResource.setAllocated(this.allocated);
        erResource.setExtention(this.extention);
        erResource.setStatus(this.status);
        return erResource;
    }

}