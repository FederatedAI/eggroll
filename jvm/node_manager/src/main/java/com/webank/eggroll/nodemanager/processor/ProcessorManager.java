package com.webank.eggroll.nodemanager.processor;

import org.fedai.eggroll.core.containers.meta.KillContainersResponse;
import org.fedai.eggroll.core.containers.meta.StartContainersResponse;
import org.fedai.eggroll.core.containers.meta.StopContainersResponse;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.*;

public interface ProcessorManager {
    ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta);

    ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta);

    ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta);

    ErProcessor heartbeat(Context context, ErProcessor processor);

    ErProcessor checkNodeProcess(Context context, ErProcessor processor);

    StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest);

    StopContainersResponse stopJobContainers(StopContainersRequest StopContainersRequest);

    KillContainersResponse killJobContainers(KillContainersRequest killContainersRequest);

    DownloadContainersResponse downloadContainers(DownloadContainersRequest downloadContainersRequest);

    StartContainersResponse startFlowJobContainers(StartFlowContainersRequest startFlowContainersRequest);
}
