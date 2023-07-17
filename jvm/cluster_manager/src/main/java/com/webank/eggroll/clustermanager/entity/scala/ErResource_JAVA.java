package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.ResourceStatus;
import com.webank.eggroll.core.constant.StringConstants;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ErResource_JAVA  implements NetworkingRpcMessage_JAVA {
    private long resourceId= -1;
    private String resourceType= StringConstants.EMPTY();
    private long serverNodeId= 0;
    private long total= -1;
    private long used= -1;
    private long allocated= -1;
    private long preAllocated= -1;
    private String extention = null;
    private String status = ResourceStatus.AVAILABLE();
    private List<String> extentionCache = new ArrayList<>();


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