package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;

public class DefaultProcessorManager implements ProcessorManager{
    @Override
    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta heartbeat(Context context, ErSessionMeta sessionMeta) {
        return null;
    }
}
