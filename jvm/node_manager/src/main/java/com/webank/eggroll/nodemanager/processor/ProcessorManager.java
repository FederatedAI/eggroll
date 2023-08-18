package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.eggroll.core.pojo.StartContainersRequest;
import com.webank.eggroll.core.meta.Containers;

public interface ProcessorManager {
    ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta );
    ErProcessor heartbeat(Context context, ErProcessor processor );
    ErProcessor checkNodeProcess(Context context, ErProcessor processor );
    Containers.StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest);
}
