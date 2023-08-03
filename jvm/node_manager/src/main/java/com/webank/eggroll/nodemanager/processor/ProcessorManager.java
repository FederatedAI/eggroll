package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;

public interface ProcessorManager {
    ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta heartbeat(Context context, ErSessionMeta sessionMeta );
}
