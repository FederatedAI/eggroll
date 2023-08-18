package com.eggroll.core.containers;


import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.containers.container.ContainersManager;
import com.eggroll.core.containers.container.DeepSpeedContainer;
import com.eggroll.core.containers.container.WarpedDeepspeedContainerConfig;
import com.eggroll.core.pojo.JobProcessorTypes;
import com.eggroll.core.pojo.StartContainersRequest;
import com.eggroll.core.pojo.StartDeepspeedContainerRequest;
import com.eggroll.core.pojo.StaticErConf;
import com.webank.eggroll.core.meta.Containers;
import lombok.val;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.springframework.stereotype.Service;

@Service
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


    public ContainersServiceHandler(ExecutorService executor, Path providedContainersDataDir) {
        this.executor = executor;
        this.providedContainersDataDir = providedContainersDataDir;
    }

    public Containers.StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest) {
        if (startContainersRequest.getJobType().equals(JobProcessorTypes.DeepSpeed)) {
            StartDeepspeedContainerRequest startDeepspeedContainerRequest =
                    StartDeepspeedContainerRequest.fromStartContainersRequest(startContainersRequest);
            return startDeepspeedContainers(startDeepspeedContainerRequest);
        } else {
            throw new IllegalArgumentException("Unsupported job type: " + startContainersRequest.getJobType());
        }
    }


    private Containers.StartContainersResponse startDeepspeedContainers(
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
        Containers.StartContainersResponse.Builder builder = Containers.StartContainersResponse.newBuilder();
        builder.setSessionId(sessionId);
        Containers.StartContainersResponse response = builder.build();
        return response;
    }

    private Path getContainerWorkspace(String sessionId, long rank) {
        return containersDataDir.resolve(sessionId).resolve(Long.toString(rank));
    }

}


