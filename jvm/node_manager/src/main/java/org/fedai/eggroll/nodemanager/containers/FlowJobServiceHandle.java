package org.fedai.eggroll.nodemanager.containers;

import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.ProcessorStatus;
import org.fedai.eggroll.core.containers.container.ContainerTrait;
import org.fedai.eggroll.core.containers.meta.StartContainersResponse;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.fedai.eggroll.core.pojo.StartFlowContainersRequest;
import com.google.inject.Singleton;
import org.fedai.eggroll.nodemanager.meta.NodeManagerMeta;
import org.fedai.eggroll.nodemanager.service.FlowProcessorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Singleton
public class FlowJobServiceHandle {

    Logger logger = LoggerFactory.getLogger(FlowJobServiceHandle.class);

    private ExecutorService executor = Executors.newWorkStealingPool();
    private Future<Void> watcher;
    private Path containersDataDir = null;
    private ClusterManagerClient client = new ClusterManagerClient(new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST, MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));

    public Path getContainersDataDir() {
        String pathStr = MetaInfo.CONFKEY_NODE_MANAGER_CONTAINERS_DATA_DIR;
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
        this.containersDataDir = path;
        return path;
    }

    /**
     * 接受flow直接提交的任务
     *
     * @return
     */
    public StartContainersResponse startFlowJobContainers(StartFlowContainersRequest startFlowContainersRequest) {
        String sessionId = startFlowContainersRequest.getSessionId();
        Map<String, String> options = startFlowContainersRequest.getOptions();
        List<String> commandArguments = startFlowContainersRequest.getCommandArguments();
        List<ErProcessor> processors = startFlowContainersRequest.getProcessors();
        String scriptPath = options.get("scriptPath");
        Map<String, String> environmentVariables = startFlowContainersRequest.getEnvironmentVariables();
        Path containerWorkspace = this.getContainersDataDir().resolve(sessionId);
        for (ErProcessor processor : processors) {
            Long processorId = processor.getId();
            try {
                FlowProcessorService flowProcessorService = new FlowProcessorService(scriptPath, containerWorkspace, environmentVariables, commandArguments, processorId);
                watcher = (Future<Void>) executor.submit(() -> {
                    flowProcessorService.start();
                    ProcessorStatus newStatus = flowProcessorService.getPid() > 0 ? ProcessorStatus.RUNNING : ProcessorStatus.ERROR;
                    client.hearbeat(new Context(), buildHeartBeatProcessor(newStatus, flowProcessorService));
                });
            } catch (Exception e) {
                logger.error("starting flow containers failed: {}", e);
                e.printStackTrace();
            }
        }
        StartContainersResponse startContainersResponse = new StartContainersResponse();
        startContainersResponse.setSessionId(sessionId);
        return startContainersResponse;
    }


    private ErProcessor buildHeartBeatProcessor(ProcessorStatus status, ContainerTrait container) {
        final ErProcessor erProcessor = new ErProcessor();
        erProcessor.setId(container.getProcessorId());
        erProcessor.setPid(container.getPid());
        erProcessor.setServerNodeId(NodeManagerMeta.serverNodeId);
        erProcessor.setStatus(status.name());
        return erProcessor;
    }

}
