package org.fedai.eggroll.nodemanager.processor;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.beanutils.BeanUtils;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.ProcessorStatus;
import org.fedai.eggroll.core.containers.meta.KillContainersResponse;
import org.fedai.eggroll.core.containers.meta.StartContainersResponse;
import org.fedai.eggroll.core.containers.meta.StopContainersResponse;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.pojo.*;
import org.fedai.eggroll.core.postprocessor.ApplicationStartedRunner;
import org.fedai.eggroll.nodemanager.containers.ContainersServiceHandler;
import org.fedai.eggroll.nodemanager.containers.FlowJobServiceHandle;
import org.fedai.eggroll.nodemanager.service.ContainerService;
import org.fedai.eggroll.nodemanager.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;


@Singleton
public class DefaultProcessorManager implements ProcessorManager, ApplicationStartedRunner {
    Logger logger = LoggerFactory.getLogger(DefaultProcessorManager.class);

    private ClusterManagerClient client;

    @Inject
    private ContainerService containerService;

    @Inject
    private ContainersServiceHandler containersServiceHandler;

    @Inject
    private FlowJobServiceHandle flowJobServiceHandle;

    @Override
    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(context, sessionMeta, Dict.NODE_CMD_START);
    }

    @Override
    public ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(context, sessionMeta, Dict.NODE_CMD_STOP);
    }

    @Override
    public ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta) {
        return containerService.operateContainers(context, sessionMeta, Dict.NODE_CMD_KILL);
    }

    @Override
    public ErProcessor heartbeat(Context context, ErProcessor processor) {
        return client.hearbeat(context, processor);
    }

    @Override
    public ErProcessor checkNodeProcess(Context context, ErProcessor processor) {
        ErProcessor result = new ErProcessor();
        try {
            BeanUtils.copyProperties(result, processor);
        } catch (InvocationTargetException | IllegalAccessException e) {
            logger.error("copyProperties error: {}", e.getMessage());
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
    public StartContainersResponse startFlowJobContainers(StartFlowContainersRequest startFlowContainersRequest) {
        return flowJobServiceHandle.startFlowJobContainers(startFlowContainersRequest);
    }

    @Override
    public StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest) {
        return containersServiceHandler.startJobContainers(startContainersRequest);
    }


    @Override
    public StopContainersResponse stopJobContainers(StopContainersRequest stopContainersRequest) {
        return containersServiceHandler.stopJobContainers(stopContainersRequest);
    }

    @Override
    public KillContainersResponse killJobContainers(KillContainersRequest killContainersRequest) {
        return containersServiceHandler.killJobContainers(killContainersRequest);
    }

    @Override
    public DownloadContainersResponse downloadContainers(DownloadContainersRequest downloadContainersRequest) {
        return containersServiceHandler.downloadContainers(downloadContainersRequest);
    }


    @Override
    public void run(String[] args) throws Exception {
        logger.info("xxxxxxxxxxxxxxxxxxxxx");
        client = new ClusterManagerClient(new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST, MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));
        logger.info("oooooooooooooooooooo");
    }
}
