package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;

public interface ProcessorManager {
    ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta );
    ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta );
    ErProcessor heartbeat(Context context, ErProcessor processor );
    ErProcessor checkNodeProcess(Context context, ErProcessor processor );
}
