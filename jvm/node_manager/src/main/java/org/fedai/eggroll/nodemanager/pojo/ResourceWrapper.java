package org.fedai.eggroll.nodemanager.pojo;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

@Data
public class ResourceWrapper {
    private String resourceType;
    AtomicLong total = new AtomicLong(0);
    AtomicLong used = new AtomicLong(0);
    AtomicLong allocated = new AtomicLong(0);

    public ResourceWrapper(String resourceType, AtomicLong total) {
        this.resourceType = resourceType;
        this.total = total;
    }
}
