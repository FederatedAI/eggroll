package com.eggroll.core.pojo;


import com.eggroll.core.config.Dict;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
@Data
public class ErResource   implements RpcMessage{
    private Long resourceId= -1L;
    private String sessionId;
    private String resourceType= Dict.EMPTY;
    private Long processorId;
    private Long serverNodeId= 0L;
    private Long total= -1L;
    private Long used= -1L;
    private Long allocated= -1L;
    private Long preAllocated= -1L;
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

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] data) {

    }
}