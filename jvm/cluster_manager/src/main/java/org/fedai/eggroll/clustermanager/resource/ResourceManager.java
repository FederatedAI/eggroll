package org.fedai.eggroll.clustermanager.resource;

import org.fedai.eggroll.core.pojo.ErProcessor;

public interface ResourceManager {
    void preAllocateResource(ErProcessor erProcessor);

    void preAllocateFailed(ErProcessor erProcessor);

    void allocatedResource(ErProcessor erProcessor);

    void returnResource(ErProcessor erProcessor);
}
