package com.webank.eggroll.clustermanager.job;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.exceptions.ErSessionException;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@Service
public class JobServiceHandler {
    Logger log = LoggerFactory.getLogger(JobServiceHandler.class);

    @Autowired
    ClusterResourceManager clusterResourceManager;
    @Autowired
    SessionMainService sessionMainService;
    @Autowired
    ServerNodeService serverNodeService;

    public void killJob(String sessionId) {
        log.info("killing job {}", sessionId);
        try {
            clusterResourceManager.lockSession(sessionId);
            clusterResourceManager.getKillJobMap().put(sessionId, System.currentTimeMillis());
            if (sessionMainService.getById(sessionId) == null) {
                return;
            }
            ErSessionMeta sessionMeta = sessionMainService.getSession(sessionId);
            if (StringUtils.equalsAny(sessionMeta.getStatus(), SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name())) {
                return;
            }
            Map<Long, List<ErProcessor>> groupMap = sessionMeta.getProcessors().stream().collect(Collectors.groupingBy(ErProcessor::getServerNodeId));
            Map<ErServerNode, List<ErProcessor>> nodeAndProcessors = new HashMap<>();
            groupMap.forEach((nodeId, processors) -> {
                ErServerNode erServerNode = serverNodeService.getById(nodeId).toErServerNode();
                KillContainersRequest killContainersRequest = new KillContainersRequest();
                killContainersRequest.setSessionId(sessionId);
                List<Long> processorIdList = new ArrayList<>();
                for (ErProcessor processor : processors) {
                    processorIdList.add(processor.getId());
                }
                try {
                    killContainersRequest.setContainers(processorIdList);
                    new NodeManagerClient(erServerNode.getEndpoint()).killJobContainers(killContainersRequest);
                } catch (Exception e) {
                    log.error("killContainers error : ", e);
                }
            });
        } finally {
            clusterResourceManager.unlockSession(sessionId);
        }
    }

//    public SubmitJobResponse handleSubmit(SubmitJobRequest submitJobMeta) {
//        if (JobProcessorTypes.DeepSpeed.name().equals(submitJobMeta.getJobType())) {
//            return handleDeepspeedSubmit(submitJobMeta);
//        } else {
//            throw new IllegalArgumentException("unsupported job type: " + submitJobMeta.getJobType());
//        }
//    }


    public QueryJobStatusResponse handleJobStatusQuery(QueryJobStatusRequest queryJobStatusRequest) {
        String sessionId = queryJobStatusRequest.getSessionId();
        ErSessionMeta sessionMain = sessionMainService.getSessionMain(sessionId);
        QueryJobStatusResponse queryJobStatusResponse = new QueryJobStatusResponse();
        queryJobStatusResponse.setSessionId(sessionId);
        queryJobStatusResponse.setStatus(sessionMain == null ? null : sessionMain.getStatus());
        return queryJobStatusResponse;
    }

    public KillJobResponse handleJobKill(KillJobRequest killJobRequest) {
        String sessionId = killJobRequest.getSessionId();
        killJob(sessionId);
        KillJobResponse response = new KillJobResponse();
        response.setSessionId(sessionId);
        return response;
    }

    public StopJobResponse handleJobStop(StopJobRequest stopJobRequest) {
        String sessionId = stopJobRequest.getSessionId();
        killJob(sessionId);
        StopJobResponse response = new StopJobResponse();
        response.setSessionId(sessionId);
        return response;
    }

    private SubmitJobResponse handleDeepspeedSubmit(SubmitJobRequest submitJobRequest) throws InterruptedException {
        String sessionId = submitJobRequest.getSessionId();
        int worldSize = submitJobRequest.getWorldSize();

        // prepare processors
        List<ErProcessor> prepareProcessors = new ArrayList<>();
        for (int i = 0; i < worldSize; i++) {
            ErProcessor erProcessor = new ErProcessor();
            erProcessor.setProcessorType(JobProcessorTypes.DeepSpeed.name());
            erProcessor.setStatus(ProcessorStatus.NEW.name());

            ErResource erResource = new ErResource();
            erResource.setResourceType(Dict.VGPU_CORE);
            erResource.setAllocated(1L);
            erResource.setStatus(ResourceStatus.PRE_ALLOCATED.name());
            erProcessor.getResources().add(erResource);

            prepareProcessors.add(erProcessor);
        }

        ResourceApplication resourceApplication = new ResourceApplication();
        resourceApplication.setSortByResourceType(Dict.VCPU_CORE);
        resourceApplication.setProcessors(prepareProcessors);
        resourceApplication.setResourceExhaustedStrategy(Dict.WAITING);
        resourceApplication.setTimeout(submitJobRequest.getResourceOptions().getTimeoutSeconds() * 1000);
        resourceApplication.setSessionId(sessionId);
        resourceApplication.setSessionName(JobProcessorTypes.DeepSpeed.toString());
        log.info("submitting resource request: " + resourceApplication + ", " + resourceApplication.hashCode());

        clusterResourceManager.submitResourceRequest(resourceApplication);
        List<Pair<ErProcessor, ErServerNode>> dispatchedProcessorList = resourceApplication.getResult();
        log.info("submitted resource request: " + resourceApplication + ", " + resourceApplication.hashCode());
        log.info("dispatchedProcessor: " + JsonUtil.object2Json(dispatchedProcessorList));

        try {
            //锁不能移到分配资源之前，会造成死锁
            clusterResourceManager.lockSession(sessionId);
            if (!clusterResourceManager.getKillJobMap().containsKey(sessionId)) {
                ErSessionMeta registeredSessionMeta = sessionMainService.getSession(submitJobRequest.getSessionId());
                List<Tuple<ErProcessor, ErServerNode, ErProcessor>> pariList = new ArrayList<>();
                //scala .zip
                for (int i = 0; i < dispatchedProcessorList.size(); i++) {
                    Pair<ErProcessor, ErServerNode> erProcessorErServerNodePair = dispatchedProcessorList.get(i);
                    ErProcessor registeredProcessor = registeredSessionMeta.getProcessors().get(i);
                    pariList.add(new Tuple<>(erProcessorErServerNodePair.getKey(), erProcessorErServerNodePair.getValue(), registeredProcessor));
                }

                List<Tuple<Long, ErServerNode, DeepspeedContainerConfig>> configs = new ArrayList<>();
                AtomicInteger globalRank = new AtomicInteger(0);
                AtomicInteger crossSize = new AtomicInteger(0);
                AtomicInteger crossRank = new AtomicInteger(0);


                Map<ErServerNode, List<Tuple<ErProcessor, ErServerNode, ErProcessor>>> collect = pariList.stream()
                        .collect(Collectors.groupingBy(Tuple::getSecond));
                collect.forEach((node, processorAndNodeArray) -> {
                    crossSize.getAndIncrement();
                    int localSize = processorAndNodeArray.size();
                    List<Integer> cudaVisibleDevices = new ArrayList<>();
                    processorAndNodeArray.forEach(pair -> {
                        String[] devicesForProcessor = pair.getFirst().getOptions().getOrDefault("cudaVisibleDevices", "-1").split(",");
                        for (String devicesStr : devicesForProcessor) {
                            int device = Integer.parseInt(devicesStr);
                            cudaVisibleDevices.add(device);
                            if (device < 0) {
                                throw new IllegalArgumentException("cudaVisibleDevices is not set or invalid: " + JsonUtil.object2Json(pair.getFirst().getOptions().get("cudaVisibleDevices")));
                            }
                        }
                    });

                    if (cudaVisibleDevices.stream().distinct().count() != cudaVisibleDevices.size()) {
                        throw new IllegalArgumentException("duplicate cudaVisibleDevices: " + JsonUtil.object2Json(cudaVisibleDevices));
                    }

                    int localRank = 0;
                    for (Tuple<ErProcessor, ErServerNode, ErProcessor> pair : processorAndNodeArray) {
                        DeepspeedContainerConfig deepspeedContainerConfig = new DeepspeedContainerConfig();
                        deepspeedContainerConfig.setCudaVisibleDevices(cudaVisibleDevices);
                        deepspeedContainerConfig.setWorldSize(worldSize);
                        deepspeedContainerConfig.setCrossRank(crossRank.get());
                        deepspeedContainerConfig.setCrossSize(crossSize.get());
                        deepspeedContainerConfig.setLocalSize(localSize);
                        deepspeedContainerConfig.setLocalRank(localRank);
                        deepspeedContainerConfig.setRank(globalRank.get());
                        deepspeedContainerConfig.setStorePrefix(sessionId);
                        configs.add(new Tuple<>(
                                pair.getFirst().getId(),
                                pair.getSecond(),
                                deepspeedContainerConfig
                        ));
                        localRank++;
                        globalRank.addAndGet(1);
                    }
                    crossRank.addAndGet(1);
                });
                sessionMainService.registerRanks(configs, sessionId);

                configs.stream().collect(Collectors.groupingBy(Tuple::getSecond)).forEach((node, nodeAndConfigs) -> {
                    NodeManagerClient nodeManagerClient = new NodeManagerClient(node.getEndpoint());
                    StartDeepspeedContainerRequest startDeepspeedContainerRequest = new StartDeepspeedContainerRequest();
                    startDeepspeedContainerRequest.setSessionId(sessionId);
                    startDeepspeedContainerRequest.setName(submitJobRequest.getName());
                    startDeepspeedContainerRequest.setCommandArguments(submitJobRequest.getCommandArguments());
                    startDeepspeedContainerRequest.setEnvironmentVariables(submitJobRequest.getEnvironmentVariables());
                    startDeepspeedContainerRequest.setFiles(submitJobRequest.getFiles());
                    startDeepspeedContainerRequest.setZippedFiles(submitJobRequest.getZippedFiles());
                    Map<Long, DeepspeedContainerConfig> deepspeedConfigs = new HashMap<>();
                    for (Tuple<Long, ErServerNode, DeepspeedContainerConfig> nodeAndConfig : nodeAndConfigs) {
                        deepspeedConfigs.put(nodeAndConfig.getFirst(), nodeAndConfig.getThird());
                    }
                    startDeepspeedContainerRequest.setDeepspeedConfigs(deepspeedConfigs);
                    startDeepspeedContainerRequest.setOptions(submitJobRequest.getOptions());
                    nodeManagerClient.startJobContainers(StartDeepspeedContainerRequest.toStartContainersRequest(startDeepspeedContainerRequest));
                });


                long startTimeout = System.currentTimeMillis() + MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS;
                List<ErProcessor> activeProcessors = waitSubmittedContainers(sessionId, worldSize, startTimeout);

                Map<Long, Map<String, String>> idToOptions = new HashMap<>();
                dispatchedProcessorList.forEach(pair->{
                    idToOptions.put(pair.getKey().getId(),pair.getKey().getOptions());
                });

                for (ErProcessor processor : activeProcessors) {
                    Map<String, String> options = idToOptions.get(processor.getId());
                    processor.getOptions().putAll(options);
                }

                UpdateWrapper<SessionMain> updateWrapper = new UpdateWrapper<>();
                updateWrapper.lambda().set(SessionMain::getStatus,SessionStatus.ACTIVE.name())
                        .eq(SessionMain::getStatus,SessionStatus.NEW.name())
                        .eq(SessionMain::getSessionId,sessionId);
                sessionMainService.update(updateWrapper);
                SubmitJobResponse submitJobResponse = new SubmitJobResponse();
                submitJobResponse.setSessionId(sessionId);
                submitJobResponse.setProcessors(activeProcessors);
                return submitJobResponse;
            } else {
                log.error("kill session " + sessionId + " request was found");
                throw new ErSessionException("kill session " + sessionId + " request was found");
            }
        } catch (Exception e) {
            killJob(sessionId);
            throw e;
        } finally {
            clusterResourceManager.unlockSession(sessionId);
        }
    }

    private List<ErProcessor> waitSubmittedContainers(String sessionId, int expectedWorldSize, long timeout) throws ErSessionException {
        boolean isStarted = false;

        while (System.currentTimeMillis() <= timeout) {
            SessionMain cur = sessionMainService.getById(sessionId);

            if (cur.getActiveProcCount() < expectedWorldSize) {
                // assert no container error
                for (ErProcessor processor : sessionMainService.getSession(sessionId).getProcessors()) {
                    if (ProcessorStatus.ERROR.name().equals(processor.getStatus())) {
                        throw new ErSessionException("processor " + processor.getId() + " failed to start");
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    // 处理中断异常（可根据具体需求处理）
                    log.error("", e);
                }
            } else {
                isStarted = true;
                break;
            }
        }

        if (!isStarted) {
            SessionMain session = sessionMainService.getById(sessionId);
            int activeCount = session.getActiveProcCount();

            if (activeCount < expectedWorldSize) {
                try {
                    killJob(sessionId);
                } catch (Exception e) {
                    log.error("failed to kill job " + sessionId, e);
                }
                throw new ErSessionException(
                        "unable to start all processors for session '" + sessionId + "', " +
                                "expected world size: " + expectedWorldSize + ", " +
                                "active world size: " + activeCount);
            }
        }

        return sessionMainService.getSession(sessionId).getProcessors();
    }
}
