package com.webank.eggroll.clustermanager.resource;

import com.eggroll.core.pojo.ErProcessor;

public interface ResourceManager {
    void preAllocateResource(ErProcessor erProcessor);

    void allocatedResource(ErProcessor erProcessor);

    void returnResource(ErProcessor erProcessor);
}
