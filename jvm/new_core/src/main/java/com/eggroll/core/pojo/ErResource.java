package com.eggroll.core.pojo;


import com.eggroll.core.config.Dict;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Data
public class ErResource implements RpcMessage {

    private Long resourceId = -1L;
    private String sessionId;
    private String resourceType = Dict.EMPTY;
    private Long processorId;
    private Long serverNodeId = 0L;
    private Long total = -1L;
    private Long used = -1L;
    private Long allocated = -1L;
    private Long preAllocated = -1L;
    private String extention = null;
    private String status = Dict.AVAILABLE;
    private List<String> extentionCache = new ArrayList<>();


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

    public Meta.Resource toProto() {
        Meta.Resource.Builder builder = Meta.Resource.newBuilder();
        builder.setType(this.resourceType)
                .setTotal(this.total)
                .setUsed(this.used)
                .setAllocated(this.allocated);
        return builder.build();
    }

    public static ErResource fromProto(Meta.Resource resource) {
        ErResource erResource = new ErResource();
        erResource.deserialize(resource.toByteArray());
        return erResource;
    }


    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.Resource resource = Meta.Resource.parseFrom(data);
            this.resourceType = resource.getType();
            this.total = resource.getTotal();
            this.used = resource.getUsed();
            this.allocated = resource.getAllocated();
        } catch (Exception e) {

        }
    }
}