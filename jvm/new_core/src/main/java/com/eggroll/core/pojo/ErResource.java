package com.eggroll.core.pojo;

import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.constant.StringConstants;

import java.util.ArrayList;
import java.util.List;

public class ErResource  {
        private long resourceId;
        private String resourceType;
        private long serverNodeId;
        private long total;
        private long used;
        private long allocated;
        private long preAllocated;
        private String extention;
        private String status;
        private List<String> extentionCache;

        public ErResource() {
            this.resourceId = -1;
            this.resourceType = StringConstants.EMPTY;
            this.serverNodeId = 0;
            this.total = -1;
            this.used = -1;
            this.allocated = -1;
            this.preAllocated = -1;
            this.extention = null;
            this.status = ResourceStatus.AVAILABLE.getValue();
            this.extentionCache = new ArrayList<>();
        }

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
