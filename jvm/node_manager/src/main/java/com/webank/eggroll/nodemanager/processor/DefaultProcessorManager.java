package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.ClusterManagerClient;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.nodemanager.service.ContainerService;
import org.springframework.stereotype.Service;
import com.webank.eggroll.nodemanager.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import javax.annotation.Resource;

@Service
public class DefaultProcessorManager implements ProcessorManager{


    Logger logger = LoggerFactory.getLogger(DefaultProcessorManager.class);
    private ClusterManagerClient client;

    @Resource
    private ContainerService containerService;

    @Override
    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(sessionMeta, Dict.NODE_CMD_START);
    }

    @Override
    public ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(sessionMeta,Dict.NODE_CMD_STOP);
    }

    @Override
    public ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(sessionMeta,Dict.NODE_CMD_KILL);
    }

    @Override
    public ErProcessor heartbeat(Context context, ErProcessor processor) {

        return client.hearbeat(processor);
    }

    @Override
    public ErProcessor checkNodeProcess(Context context, ErProcessor processor) {
        ErProcessor result = new ErProcessor();
        BeanUtils.copyProperties(processor, result);
        if (ProcessUtils.checkProcess(Integer.toString(processor.getPid()))) {
            result.setStatus(ProcessorStatus.RUNNING.name());
        } else {
            result.setStatus(ProcessorStatus.KILLED.name());
        }
        logger.info("check processor pid " + processor.getPid() + " return " + result.getStatus());
        return result;
    }

}
