package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.nodemanager.service.ContainerService;

import javax.annotation.Resource;

public class DefaultProcessorManager implements ProcessorManager{

    @Resource
    private ContainerService containerService;

    @Override
    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(sessionMeta, Dict.NODE_CMD_START);
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
