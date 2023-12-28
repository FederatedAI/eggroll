package org.fedai.eggroll.nodemanager.containers;


import com.google.gson.Gson;
import com.google.inject.Singleton;
import com.webank.eggroll.core.meta.Containers;
import com.webank.eggroll.core.transfer.Extend;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.ExtendEnvConf;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.ProcessorStatus;
import org.fedai.eggroll.core.containers.container.*;
import org.fedai.eggroll.core.containers.meta.KillContainersResponse;
import org.fedai.eggroll.core.containers.meta.StartContainersResponse;
import org.fedai.eggroll.core.containers.meta.StopContainersResponse;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.exceptions.PathNotExistException;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.pojo.*;
import org.fedai.eggroll.nodemanager.extend.LogStreamHolder;
import org.fedai.eggroll.nodemanager.meta.NodeManagerMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Singleton
public class ContainersServiceHandler {

    Logger logger = LoggerFactory.getLogger(ContainersServiceHandler.class);

    private ExecutorService executor = Executors.newWorkStealingPool();

    private ClusterManagerClient client = new ClusterManagerClient(new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST, MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));

    private ContainersManager containersManager = buildContainersManager();

    private Path containersDataDir = null;

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


    public StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest) {
        if (startContainersRequest.getJobType() != null) {
            if (startContainersRequest.getJobType().equals(JobProcessorTypes.DeepSpeed.getName())) {
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


    private StartContainersResponse startDeepspeedContainers(StartDeepspeedContainerRequest startDeepspeedContainerRequest) {
        String sessionId = startDeepspeedContainerRequest.getSessionId();
        logger.info("(sessionId=" + sessionId + ") starting deepspeed containers");
        startDeepspeedContainerRequest.getDeepspeedConfigs().forEach((containerId, deepspeedConfig) -> {
            Integer rank = deepspeedConfig.getRank();
            WarpedDeepspeedContainerConfig warpedDeepspeedContainerConfig = new WarpedDeepspeedContainerConfig(deepspeedConfig);
            // add resource env
            warpedDeepspeedContainerConfig.setEggrollContainerResourceLogsDir(getContainerLogsDir(sessionId, rank).toString());
            warpedDeepspeedContainerConfig.setEggrollContainerResourceModelsDir(getContainerModelsDir(sessionId, rank).toString());
            warpedDeepspeedContainerConfig.setEggrollContainerResourceResultDir(getContainerResultDir(sessionId, rank).toString());
            Map<String, String> envMap = new HashMap<>();
            envMap.putAll(startDeepspeedContainerRequest.getEnvironmentVariables());
            envMap.putAll(ExtendEnvConf.confMap);
            logger.info("containerId :{} env map :{}", containerId, envMap);
            try {
                DeepSpeedContainer container = new DeepSpeedContainer(
                        sessionId,
                        containerId,
                        warpedDeepspeedContainerConfig,
                        getContainerWorkspace(sessionId, deepspeedConfig.getRank()),
                        startDeepspeedContainerRequest.getCommandArguments(),
                        envMap,
                        startDeepspeedContainerRequest.getFiles(),
                        startDeepspeedContainerRequest.getZippedFiles(),
                        startDeepspeedContainerRequest.getOptions()
                );
                containersManager.addContainer(containerId, container);
                containersManager.startContainer(containerId);
            } catch (Exception e) {
                logger.error(" starting deepspeed containers failed: {}", e);
                e.printStackTrace();
            }
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
        Gson gson = new Gson();
        String killContainersRequestStr = gson.toJson(killContainersRequest);
        logger.info("====================killJobContainers==============reqParam: {}", killContainersRequestStr);
        logger.info("(sessionId=" + sessionId + ") killing containers");
        for (Long containerId : killContainersRequest.getContainers()) {
            logger.info("to kill container {}", containerId);
            containersManager.killContainer(containerId);
        }
        KillContainersResponse killContainersResponse = new KillContainersResponse();
        killContainersResponse.setSessionId(sessionId);
        return killContainersResponse;
    }

    public DownloadContainersResponse downloadContainers(DownloadContainersRequest downloadContainersRequest) {
        String sessionId = downloadContainersRequest.getSessionId();
        Containers.ContentType contentType = downloadContainersRequest.getContentType();
        List<Integer> ranks = downloadContainersRequest.getRanks();
        String compressMethod = downloadContainersRequest.getCompressMethod();
        int level = downloadContainersRequest.getCompressLevel();
        logger.info("downloading containers, sessionId: {}, ranks: {}", sessionId, ranks.stream().map(Object::toString).collect(Collectors.joining(",")));

        List<ContainerContent> contents = ranks.stream()
                .map(rank -> {
                    Path targetDir;
                    if (contentType.equals(Containers.ContentType.ALL)) {
                        targetDir = getContainerWorkspace(sessionId, rank);
                    } else if (contentType.equals(Containers.ContentType.MODELS)) {
                        targetDir = getContainerModelsDir(sessionId, rank);
                    } else if (contentType.equals(Containers.ContentType.LOGS)) {
                        targetDir = getContainerLogsDir(sessionId, rank);
                    } else {
                        throw new IllegalArgumentException("unsupported container content type: " + contentType);
                    }
                    if (compressMethod.equals(Dict.ZIP)) {
                        if (Files.exists(targetDir)) {
                            return new ContainerContent(rank, zip(targetDir, level), compressMethod);
                        } else {
                            return new ContainerContent(rank, new byte[0], compressMethod);
                        }
                    } else {
                        throw new IllegalArgumentException("compress method not supported: " + compressMethod);
                    }
                }).collect(Collectors.toList());

        return new DownloadContainersResponse(sessionId, contents);
    }

    private byte[] zip(Path path, int level) {
        logger.info("zipping path: " + path.toString());
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ZipOutputStream zipOutput = new ZipOutputStream(byteStream);
        zipOutput.setLevel(level);
        try {
            Files.walk(path).forEach(subPath -> {
                if (Files.isRegularFile(subPath)) {
                    String name = path.relativize(subPath).toString();

                    try {
                        zipOutput.putNextEntry(new ZipEntry(name));
                        FileInputStream inputStream = new FileInputStream(subPath.toFile());
                        byte[] buffer = new byte[1024];
                        int bytesRead = inputStream.read(buffer);
                        while (bytesRead != -1) {
                            zipOutput.write(buffer, 0, bytesRead);
                            bytesRead = inputStream.read(buffer);
                        }
                        inputStream.close();

                        zipOutput.closeEntry();
                    } catch (Exception e) {
                        logger.error("zip file failed: {}", e.getMessage());
                    }
                }
            });
        } catch (IOException e) {
            logger.error("zip file failed: {}", e.getMessage());
        } finally {
            try {
                zipOutput.close();
            } catch (Exception e) {
                logger.error("zip file failed: {}", e.getMessage());
            }
        }
        logger.info("zipped path: " + path.toString());
        return byteStream.toByteArray();
    }

    private Path getContainerWorkspace(String sessionId, long rank) {
        return this.getContainersDataDir().resolve(sessionId).resolve(Long.toString(rank));
    }

    private Path getContainerModelsDir(String sessionId, long rank) {
        return getContainerWorkspace(sessionId, rank).resolve(Dict.MODELS).toAbsolutePath();
    }

    private Path getContainerLogsDir(String sessionId, long rank) {
        return getContainerWorkspace(sessionId, rank).resolve(Dict.LOGS).toAbsolutePath();
    }

    private Path getContainerResultDir(String sessionId, long rank) {
        return getContainerWorkspace(sessionId, rank).resolve(Dict.RESULT).toAbsolutePath();
    }


    public LogStreamHolder createLogStream(Extend.GetLogRequest request, StreamObserver<Extend.GetLogResponse> responseObserver) throws PathNotExistException {
        String sessionId = request.getSessionId();
        long line = request.getStartLine() > 0 ? request.getStartLine() : 200;
        int rank = Integer.valueOf(request.getRank());

        // 获取日志文件路径
        Path path = getContainerLogsDir(sessionId, rank);
        path = path.resolve((StringUtils.isNotBlank(request.getLogType()) ? request.getLogType() : "INFO") + ".log");

        if (!path.toFile().exists()) {
            throw new PathNotExistException("Can not find file " + path);
        }

        String command = "tail -F -n " + line + " " + path.toString();
        return new LogStreamHolder(System.currentTimeMillis(), command, responseObserver, "running");
    }

    private ContainersManager buildContainersManager() {
        final ContainersManagerBuilder builder = ContainersManager.builder();
        builder.withStartedCallback((status, container, exception) -> {
            ProcessorStatus newStatus = container.getPid() > 0 ? ProcessorStatus.RUNNING : ProcessorStatus.ERROR;
            client.hearbeat(new Context(), buildHeartBeatProcessor(newStatus, container));
        });
        builder.withSuccessCallback((status, container, exception) -> {
            client.hearbeat(new Context(), buildHeartBeatProcessor(ProcessorStatus.FINISHED, container));
        });
        builder.withFailedCallback((status, container, exception) -> {
            client.hearbeat(new Context(), buildHeartBeatProcessor(ProcessorStatus.ERROR, container));
        });
        builder.withExceptionCallback((status, container, exception) -> {
            client.hearbeat(new Context(), buildHeartBeatProcessor(ProcessorStatus.KILLED, container));
        });

        return builder.build(executor);
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


