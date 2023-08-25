package com.webank.eggroll.nodemanager.containers;


import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.containers.container.ContainersManager;
import com.eggroll.core.containers.container.DeepSpeedContainer;
import com.eggroll.core.containers.container.WarpedDeepspeedContainerConfig;
import com.eggroll.core.containers.meta.KillContainersResponse;
import com.eggroll.core.containers.meta.StartContainersResponse;
import com.eggroll.core.containers.meta.StopContainersResponse;
import com.eggroll.core.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;


public class ContainersServiceHandler {

    Logger logger = LoggerFactory.getLogger(ContainersServiceHandler.class);

    private ExecutorService executor;

    private ContainersManager containersManager = ContainersManager.builder().build(executor);
    private StartDeepspeedContainerRequest startDeepspeedContainerRequest;


    private Path providedContainersDataDir;

    private Path containersDataDir = null;

    private synchronized Path getContainersDataDir() {
        if (containersDataDir == null) {
            String providedDataDir = providedContainersDataDir != null ? String.valueOf(providedContainersDataDir) : null;
            if (providedDataDir == null) {
                String pathStr = StaticErConf.getString(MetaInfo.CONFKEY_NODE_MANAGER_CONTAINERS_DATA_DIR, "");

                if (pathStr == null || pathStr.isEmpty()) {
                    throw new IllegalArgumentException("container data dir not set");
                }
                Path path = Paths.get(pathStr);
                if (!Files.exists(path)) {
                    try {
                        Files.createDirectory(path);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                containersDataDir = path;
            } else {
                containersDataDir = Paths.get(providedDataDir);
            }
        }
        return containersDataDir;
    }


    public ContainersServiceHandler(ExecutorService executorService, Path providedContainersDataDir) {
        this.executor = executorService;
        this.providedContainersDataDir = providedContainersDataDir;
    }


    public StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest) {
        if (startContainersRequest.getJobType() != null) {
            if (startContainersRequest.getJobType().equals(JobProcessorTypes.DeepSpeed.name())) {
                StartDeepspeedContainerRequest startDeepspeedContainerRequest =
                        StartDeepspeedContainerRequest.fromStartContainersRequest(startContainersRequest);
                return startDeepspeedContainers(startDeepspeedContainerRequest);
            } else {
                throw new IllegalArgumentException("unsupported job type: " + startContainersRequest.getJobType().toString());
            }
        } else {
            throw new IllegalArgumentException("job type is missing");
        }
    }

    private StartContainersResponse startDeepspeedContainers(
            StartDeepspeedContainerRequest startDeepspeedContainerRequest) {
        String sessionId = startDeepspeedContainerRequest.getSessionId();
        logger.info("(sessionId=" + sessionId + ") starting deepspeed containers");

        startDeepspeedContainerRequest.getDeepspeedConfigs().forEach((containerId, deepspeedConfig) -> {
            WarpedDeepspeedContainerConfig warpedDeepspeedContainerConfig =
                    new WarpedDeepspeedContainerConfig(deepspeedConfig);
            DeepSpeedContainer container = null;
            try {
                container = new DeepSpeedContainer(
                        sessionId,
                        containerId,
                        warpedDeepspeedContainerConfig,
                        getContainerWorkspace(sessionId, deepspeedConfig.getRank()),
                        startDeepspeedContainerRequest.getCommandArguments(),
                        startDeepspeedContainerRequest.getEnvironmentVariables(),
                        startDeepspeedContainerRequest.getFiles(),
                        startDeepspeedContainerRequest.getZippedFiles(),
                        startDeepspeedContainerRequest.getOptions()
                );
            } catch (Exception e) {
                e.printStackTrace();
            }

            containersManager.addContainer(containerId, container);
            containersManager.startContainer(containerId);
            logger.info("(sessionId=" + sessionId + ") deepspeed container started: " + containerId);
        });

        logger.info("(sessionId=" + sessionId + ") deepspeed co started");
        StartContainersResponse startContainersResponse = new StartContainersResponse();
        startContainersResponse.setSessionId(sessionId);
        return startContainersResponse;
    }


    public StopContainersResponse stopJobContainers(StopContainersRequest stopContainersRequest) {
        String sessionId = stopContainersRequest.getSessionId();
        logger.info("(sessionId=" + stopContainersRequest.getSessionId() + ")stopping containers");
        for (Long containerId : stopContainersRequest.getContainers()) {
            containersManager.stopContainer(containerId);
        }
        return new StopContainersResponse(sessionId);
    }

    public KillContainersResponse killJobContainers(KillContainersRequest killContainersRequest) {
        String sessionId = killContainersRequest.getSessionId();
        logger.info("(sessionId=" + sessionId + ") killing containers");
        for (Long containerId : killContainersRequest.getContainers()) {
            containersManager.killContainer(containerId);
        }
        KillContainersResponse killContainersResponse = new KillContainersResponse();
        killContainersResponse.setSessionId(sessionId);
        return killContainersResponse;
    }

    private Path getContainerWorkspace(String sessionId, long rank) {
        return containersDataDir.resolve(sessionId).resolve(Long.toString(rank));
    }


}


