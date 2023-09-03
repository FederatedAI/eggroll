package com.webank.eggroll.nodemanager.processor;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.containers.meta.KillContainersResponse;
import com.eggroll.core.containers.meta.StartContainersResponse;
import com.eggroll.core.containers.meta.StopContainersResponse;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.ClusterManagerClient;
import com.eggroll.core.pojo.*;
import com.eggroll.core.postprocessor.ApplicationStartedRunner;
import com.eggroll.core.utils.JsonUtil;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.nodemanager.containers.ContainersServiceHandler;
import com.webank.eggroll.nodemanager.service.ContainerService;
import com.webank.eggroll.nodemanager.utils.ProcessUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;


@Singleton
public class DefaultProcessorManager   implements ProcessorManager,ApplicationStartedRunner {
    Logger logger = LoggerFactory.getLogger(DefaultProcessorManager.class);

    private ClusterManagerClient client;

    @Inject
    private ContainerService containerService;

    @Inject
    private ContainersServiceHandler containersServiceHandler;

    @Override
    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        logger.info("[startContainers]=====sessionMeta: {}", JsonUtil.object2Json(sessionMeta));
        return containerService.operateContainers(sessionMeta, Dict.NODE_CMD_START);
    }

    @Override
    public ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta) {
        logger.info("[stopContainers]=====sessionMeta: {}", JsonUtil.object2Json(sessionMeta));
        return containerService.operateContainers(sessionMeta,Dict.NODE_CMD_STOP);
    }

    @Override
    public ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta) {
        logger.info("[killContainers]=====sessionMeta: {}", JsonUtil.object2Json(sessionMeta));
        return containerService.operateContainers(sessionMeta,Dict.NODE_CMD_KILL);
    }

    @Override
    public ErProcessor heartbeat(Context context, ErProcessor processor) {
        logger.info("[heartbeat]=====processor: {}", JsonUtil.object2Json(processor));
        return client.hearbeat(processor);
    }

    @Override
    public ErProcessor checkNodeProcess(Context context, ErProcessor processor){
        logger.info("[checkNodeProcess]=====processor: {}", JsonUtil.object2Json(processor));
        ErProcessor result = new ErProcessor();
        try {
            BeanUtils.copyProperties(result,processor);
        }catch (InvocationTargetException | IllegalAccessException e ) {
            logger.error("copyProperties error: {}",e.getMessage());
        }

        if (ProcessUtils.checkProcess(Integer.toString(processor.getPid()))) {
            result.setStatus(ProcessorStatus.RUNNING.name());
        } else {
            result.setStatus(ProcessorStatus.KILLED.name());
        }
        logger.info("check processor pid " + processor.getPid() + " return " + result.getStatus());
        return result;
    }

    @Override
    public StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest) {
        logger.info("[startJobContainers]=====startContainersRequest: {}", JsonUtil.object2Json(startContainersRequest));
        return containersServiceHandler.startJobContainers(startContainersRequest);
    }


    @Override
    public StopContainersResponse stopJobContainers(StopContainersRequest stopContainersRequest) {
        logger.info("[stopJobContainers]=====stopContainersRequest: {}", JsonUtil.object2Json(stopContainersRequest));
        return containersServiceHandler.stopJobContainers(stopContainersRequest);
    }

    @Override
    public KillContainersResponse killJobContainers(KillContainersRequest killContainersRequest) {
        logger.info("[killJobContainers]=====killContainersRequest: {}", JsonUtil.object2Json(killContainersRequest));
        return containersServiceHandler.killJobContainers(killContainersRequest);
    }


    @Override
    public void run(String[] args) throws Exception {
        client =  new ClusterManagerClient(new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST,MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));
    }
}
