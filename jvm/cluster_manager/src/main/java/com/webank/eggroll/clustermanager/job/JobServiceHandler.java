package com.webank.eggroll.clustermanager.job;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ProcessorType;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.containers.container.Container;
import com.eggroll.core.context.Context;
import com.eggroll.core.exceptions.ErSessionException;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.dao.impl.SessionRanksService;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.SessionMain;

import com.webank.eggroll.clustermanager.entity.SessionRanks;
import com.webank.eggroll.core.meta.Containers;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Singleton
public class JobServiceHandler {
    Logger log = LoggerFactory.getLogger(JobServiceHandler.class);

    @Inject
    ClusterResourceManager clusterResourceManager;
    @Inject
    SessionMainService sessionMainService;
    @Inject
    ServerNodeService serverNodeService;

    @Inject
    SessionRanksService sessionRanksService;

    public void killJob(Context context, String sessionId) {
        log.info("killing job {}", sessionId);
        try {
            clusterResourceManager.lockSession(sessionId);
            clusterResourceManager.getKillJobMap().put(sessionId, System.currentTimeMillis());
            if (sessionMainService.getById(sessionId) == null) {
                log.error("can not found session {} ",sessionId);
                return;
            }
            ErSessionMeta sessionMeta = sessionMainService.getSession(sessionId);
            if (StringUtils.equalsAny(sessionMeta.getStatus(),
                    SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name(),SessionStatus.FINISHED.name())) {
                log.error(" session {} status is {}, will not send kill request to nodemanager",sessionId,sessionMeta.getStatus());
                return;
            }

            Map<Long, List<ErProcessor>> groupMap = sessionMeta.getProcessors().stream().collect(Collectors.groupingBy(ErProcessor::getServerNodeId));

            log.info("xxxxxxxxxxxxxxx 1111 {}",groupMap);
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
                    new NodeManagerClient(erServerNode.getEndpoint()).killJobContainers(context, killContainersRequest);
                } catch (Exception e) {
                    log.error("killContainers error : ", e);
                }
            });
        } finally {
            clusterResourceManager.unlockSession(sessionId);
        }
    }

    public SubmitJobResponse handleSubmit(SubmitJobRequest submitJobMeta) throws InterruptedException {
        if (JobProcessorTypes.DeepSpeed.getName().equals(submitJobMeta.getJobType())) {
            return handleDeepspeedSubmit(submitJobMeta);
        } else {
            throw new IllegalArgumentException("unsupported job type: " + submitJobMeta.getJobType());
        }
    }


    public QueryJobStatusResponse handleJobStatusQuery(QueryJobStatusRequest queryJobStatusRequest) {
        String sessionId = queryJobStatusRequest.getSessionId();
        ErSessionMeta sessionMain = sessionMainService.getSessionMain(sessionId);
        QueryJobStatusResponse queryJobStatusResponse = new QueryJobStatusResponse();
        queryJobStatusResponse.setSessionId(sessionId);
        queryJobStatusResponse.setStatus(sessionMain == null ? null : sessionMain.getStatus());
        return queryJobStatusResponse;
    }

    public QueryJobResponse handleJobQuery(QueryJobRequest queryJobRequest) {
        SessionMain sessionMain = sessionMainService.getById(queryJobRequest.getSessionId());
        QueryJobResponse queryJobResponse = new QueryJobResponse();
        queryJobResponse.setSessionId(queryJobRequest.getSessionId());
        if (sessionMain != null) {
            queryJobResponse.setStatus(sessionMain.getStatus());
        }
        ErSessionMeta erSession = sessionMainService.getSession(queryJobRequest.getSessionId());
        if (erSession != null) {
            queryJobResponse.setProcessors(erSession.getProcessors());
        }
        return queryJobResponse;
    }


    public KillJobResponse handleJobKill(Context context, KillJobRequest killJobRequest) {
        String sessionId = killJobRequest.getSessionId();
        killJob(context, sessionId);
        KillJobResponse response = new KillJobResponse();
        response.setSessionId(sessionId);
        return response;
    }

    public StopJobResponse handleJobStop(Context context, StopJobRequest stopJobRequest) {
        String sessionId = stopJobRequest.getSessionId();
        killJob(context, sessionId);
        StopJobResponse response = new StopJobResponse();
        response.setSessionId(sessionId);
        return response;
    }

    public SubmitJobResponse handleDeepspeedSubmit(SubmitJobRequest submitJobRequest) throws InterruptedException {
        String sessionId = submitJobRequest.getSessionId();
        int worldSize = submitJobRequest.getWorldSize();

        // prepare processors
        List<ErProcessor> prepareProcessors = new ArrayList<>();
        for (int i = 0; i < worldSize; i++) {
            ErProcessor erProcessor = new ErProcessor();
            erProcessor.setProcessorType(JobProcessorTypes.DeepSpeed.getName());
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
        List<MutablePair<ErProcessor, ErServerNode>> dispatchedProcessorList = resourceApplication.getResult();
        log.info("submitted resource request: " + resourceApplication + ", " + resourceApplication.hashCode());
        log.info("dispatchedProcessor: " + JsonUtil.object2Json(dispatchedProcessorList));

        try {
            //锁不能移到分配资源之前，会造成死锁
            clusterResourceManager.lockSession(sessionId);
            if (!clusterResourceManager.getKillJobMap().containsKey(sessionId)) {
                ErSessionMeta registeredSessionMeta = sessionMainService.getSession(submitJobRequest.getSessionId());
                List<MutableTriple<ErProcessor, ErServerNode, ErProcessor>> pariList = new ArrayList<>();
                //scala .zip
                for (int i = 0; i < dispatchedProcessorList.size(); i++) {
                    MutablePair<ErProcessor, ErServerNode> erProcessorErServerNodePair = dispatchedProcessorList.get(i);
                    ErProcessor registeredProcessor = registeredSessionMeta.getProcessors().get(i);
                    pariList.add(new MutableTriple<>(erProcessorErServerNodePair.getKey(), erProcessorErServerNodePair.getValue(), registeredProcessor));
                }

                List<MutableTriple<Long, ErServerNode, DeepspeedContainerConfig>> configs = new ArrayList<>();
                AtomicInteger globalRank = new AtomicInteger(0);
                AtomicInteger crossSize = new AtomicInteger(0);
                AtomicInteger crossRank = new AtomicInteger(0);


                Map<ErServerNode, List<MutableTriple<ErProcessor, ErServerNode, ErProcessor>>> collect = pariList.stream()
                        .collect(Collectors.groupingBy(MutableTriple::getMiddle));
                collect.forEach((node, processorAndNodeArray) -> {
                    crossSize.getAndIncrement();
                    int localSize = processorAndNodeArray.size();
                    List<Integer> cudaVisibleDevices = new ArrayList<>();
                    processorAndNodeArray.forEach(pair -> {
                        String[] devicesForProcessor = pair.getLeft().getOptions().getOrDefault("cudaVisibleDevices", "-1").split(",");
                        for (String devicesStr : devicesForProcessor) {
                            int device = Integer.parseInt(devicesStr);
                            cudaVisibleDevices.add(device);
                            if (device < 0) {
                                throw new IllegalArgumentException("cudaVisibleDevices is not set or invalid: " + JsonUtil.object2Json(pair.getLeft().getOptions().get("cudaVisibleDevices")));
                            }
                        }
                    });

                    if (cudaVisibleDevices.stream().distinct().count() != cudaVisibleDevices.size()) {
                        throw new IllegalArgumentException("duplicate cudaVisibleDevices: " + JsonUtil.object2Json(cudaVisibleDevices));
                    }

                    int localRank = 0;
                    for (MutableTriple<ErProcessor, ErServerNode, ErProcessor> pair : processorAndNodeArray) {
                        DeepspeedContainerConfig deepspeedContainerConfig = new DeepspeedContainerConfig();
                        deepspeedContainerConfig.setCudaVisibleDevices(cudaVisibleDevices);
                        deepspeedContainerConfig.setWorldSize(worldSize);
                        deepspeedContainerConfig.setCrossRank(crossRank.get());
                        deepspeedContainerConfig.setCrossSize(crossSize.get());
                        deepspeedContainerConfig.setLocalSize(localSize);
                        deepspeedContainerConfig.setLocalRank(localRank);
                        deepspeedContainerConfig.setRank(globalRank.get());
                        deepspeedContainerConfig.setStorePrefix(sessionId);
                        configs.add(new MutableTriple<>(
                                pair.getLeft().getId(),
                                pair.getMiddle(),
                                deepspeedContainerConfig
                        ));
                        localRank++;
                        globalRank.addAndGet(1);
                    }
                    crossRank.addAndGet(1);
                });
                sessionMainService.registerRanks(configs, sessionId);

                configs.stream().collect(Collectors.groupingBy(MutableTriple::getMiddle)).forEach((node, nodeAndConfigs) -> {
                    NodeManagerClient nodeManagerClient = new NodeManagerClient(node.getEndpoint());
                    StartDeepspeedContainerRequest startDeepspeedContainerRequest = new StartDeepspeedContainerRequest();
                    startDeepspeedContainerRequest.setSessionId(sessionId);
                    startDeepspeedContainerRequest.setName(submitJobRequest.getName());
                    startDeepspeedContainerRequest.setCommandArguments(submitJobRequest.getCommandArguments());
                    startDeepspeedContainerRequest.setEnvironmentVariables(submitJobRequest.getEnvironmentVariables());
                    startDeepspeedContainerRequest.setFiles(submitJobRequest.getFiles());
                    startDeepspeedContainerRequest.setZippedFiles(submitJobRequest.getZippedFiles());
                    Map<Long, DeepspeedContainerConfig> deepspeedConfigs = new HashMap<>();
                    for (MutableTriple<Long, ErServerNode, DeepspeedContainerConfig> nodeAndConfig : nodeAndConfigs) {
                        deepspeedConfigs.put(nodeAndConfig.getLeft(), nodeAndConfig.getRight());
                    }
                    startDeepspeedContainerRequest.setDeepspeedConfigs(deepspeedConfigs);
                    startDeepspeedContainerRequest.setOptions(submitJobRequest.getOptions());
                    nodeManagerClient.startJobContainers(new Context(), StartDeepspeedContainerRequest.toStartContainersRequest(startDeepspeedContainerRequest));
                });


                long startTimeout = System.currentTimeMillis() + MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS;
                List<ErProcessor> activeProcessors = waitSubmittedContainers(sessionId, worldSize, startTimeout);

                Map<Long, Map<String, String>> idToOptions = new HashMap<>();
                dispatchedProcessorList.forEach(pair -> {
                    idToOptions.put(pair.getKey().getId(), pair.getKey().getOptions());
                });

                for (ErProcessor processor : activeProcessors) {
                    Map<String, String> options = idToOptions.get(processor.getId());
                    processor.getOptions().putAll(options);
                }

                UpdateWrapper<SessionMain> updateWrapper = new UpdateWrapper<>();
                updateWrapper.lambda().set(SessionMain::getStatus, SessionStatus.ACTIVE.name())
                        .eq(SessionMain::getStatus, SessionStatus.NEW.name())
                        .eq(SessionMain::getSessionId, sessionId);
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
            killJob(new Context(), sessionId);
            throw e;
        } finally {
            clusterResourceManager.unlockSession(sessionId);
        }
    }

    public List<ErProcessor> waitSubmittedContainers(String sessionId, int expectedWorldSize, long timeout) throws ErSessionException {
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
                    log.info("waiting processor :{}  start ",sessionId);
                    Thread.sleep(3000);
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
                    killJob(new Context(), sessionId);
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


    public DownloadJobResponse handleJobDownload(Context context, DownloadJobRequest downloadJobRequest) {

        String sessionId = downloadJobRequest.getSessionId();
        Containers.ContentType contentType = downloadJobRequest.getContentType();
        String compressMethod = downloadJobRequest.getCompressMethod();
        List<Integer> ranksList = downloadJobRequest.getRanks();
        Integer compressLevel = downloadJobRequest.getCompressLevel();

        SessionRanks sessionRank = new SessionRanks();
        sessionRank.setSessionId(sessionId);

        List<SessionRanks> sessionRanksList = sessionRanksService.list(sessionRank);

        Map<Long, List<SessionRanksTemp>> rankMap = sessionRanksList.stream().flatMap(sessionRanks -> {
            Long containerId = sessionRanks.getContainerId();
            Long serverNodeId = sessionRanks.getServerNodeId();
            Integer globalRank = sessionRanks.getGlobalRank();
            Integer localRank = sessionRanks.getLocalRank();
            int index = CollectionUtils.isEmpty(ranksList) ? globalRank : ranksList.indexOf(globalRank);
            if (index >= 0) {
                //  Some((nodeId, containerId, globalRank, localRank, index))
                return Arrays.asList(new SessionRanksTemp(serverNodeId, containerId, globalRank, localRank, index)).stream();
            } else {
                return null;
            }
        }).collect(Collectors.groupingBy(SessionRanksTemp::getServerNodeId));


        List<IndexContentsTemp> IndexContentsTempList = rankMap.entrySet().stream().flatMap(entry -> {
            Long nodeId = entry.getKey();
            List<SessionRanksTemp> ranks = entry.getValue();
            ErServerNode erServerNode = serverNodeService.getByIdFromCache(nodeId);
            List<Integer> indexes = ranks.stream().map(SessionRanksTemp::getIndex).collect(Collectors.toList());
            List<Integer> globalRanks = ranks.stream().map(SessionRanksTemp::getGlobalRank).collect(Collectors.toList());
            try {
                NodeManagerClient nodeManagerClient = new NodeManagerClient(erServerNode.getEndpoint());
                DownloadContainersRequest downloadContainersRequest =
                        new DownloadContainersRequest(sessionId, compressMethod, globalRanks, compressLevel, contentType);

                DownloadContainersResponse response = nodeManagerClient.downloadContainers(new Context(), downloadContainersRequest);
                List<ContainerContent> containerContents = response.getContainerContents();
                IndexContentsTemp indexContentsTemp = new IndexContentsTemp(indexes, containerContents);
                return Arrays.asList(indexContentsTemp).stream();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());

        List<IndexContentTemp> collect = IndexContentsTempList.stream().flatMap(indexContentsTemp -> {
            List<ContainerContent> containerContents = indexContentsTemp.getContainerContents();
            List<Integer> indexes = indexContentsTemp.getIndexes();
            return IntStream.range(0, Math.min(containerContents.size(), indexes.size())).mapToObj(i -> {
                return new IndexContentTemp(indexes.get(i), containerContents.get(i));
            });
        }).sorted((p1, p2) -> {
            return p1.getIndex() - p2.getIndex();
        }).collect(Collectors.toList());

        List<ContainerContent> respContainerContents = collect.stream().map(IndexContentTemp::getContainerContent).collect(Collectors.toList());
        DownloadJobResponse downloadJobResponse = new DownloadJobResponse(sessionId, respContainerContents);
        return downloadJobResponse;
    }


    public PrepareJobDownloadResponse prepareJobDownload(Context context, PrepareJobDownloadRequest prepareRequest) {
        String sessionId = prepareRequest.getSessionId();
        HashMap<Object, Object> contentMap = new HashMap<>();
        List<ErProcessor> processors = new ArrayList<>();

        SessionRanks sessionRank = new SessionRanks();
        sessionRank.setSessionId(sessionId);
        List<SessionRanks> sessionRanksList = sessionRanksService.list(sessionRank);
        List<Integer> ranksList = prepareRequest.getRanks();

        Map<Long, List<SessionRanksTemp>> nodeIdMeta = sessionRanksList.stream().flatMap(sessionRanks -> {
            Long containerId = sessionRanks.getContainerId();
            Long serverNodeId = sessionRanks.getServerNodeId();
            Integer globalRank = sessionRanks.getGlobalRank();
            Integer localRank = sessionRanks.getLocalRank();
            int index = CollectionUtils.isEmpty(ranksList) ? globalRank : ranksList.indexOf(globalRank);
            if (index >= 0) {
                return Arrays.asList(new SessionRanksTemp(serverNodeId, containerId, globalRank, localRank, index)).stream();
            } else {
                return null;
            }
        }).collect(Collectors.groupingBy(SessionRanksTemp::getServerNodeId));

        nodeIdMeta.entrySet().forEach(t -> {
            Long serverNodeId = t.getKey();
            List<SessionRanksTemp> sessionRanks = t.getValue();
            HashMap<String, String> options = new HashMap<>();
            ServerNode serverNodeQuery = new ServerNode();
            serverNodeQuery.setServerNodeId(serverNodeId);

            ServerNode serverNodeInDb = serverNodeService.get(serverNodeQuery);
            if (null != serverNodeInDb) {
                options.put(Dict.IP, serverNodeInDb.getHost());
                options.put(Dict.PORT, serverNodeInDb.getPort().toString());

                contentMap.put(serverNodeInDb.getServerNodeId().toString(), sessionRanks);
                ErProcessor erProcessor = new ErProcessor();
                erProcessor.setSessionId(sessionId);
                erProcessor.setServerNodeId(serverNodeId);
                erProcessor.setProcessorType(ProcessorType.egg_pair.name());
                erProcessor.setName(Dict.DS_DOWNLOAD);
                erProcessor.setStatus(ProcessorStatus.NEW.name());
                erProcessor.setOptions(options);
                processors.add(erProcessor);
            }
        });

        if (CollectionUtils.isEmpty(processors)) {
            throw new ErSessionException("can not find download rank info for session " + sessionId);
        }

        String newSessionId = "DS-DOWNLOAD-" + System.currentTimeMillis() + "-" + new Random().nextInt(100);
        ResourceApplication resourceApplication = new ResourceApplication();
        resourceApplication.setSessionId(newSessionId);
        resourceApplication.setProcessors(processors);

        ErSessionMeta newErSessionMeta = clusterResourceManager.submitJodDownload(resourceApplication);

        if (!SessionStatus.ACTIVE.name().equals(newErSessionMeta.getStatus())) {
            throw new ErSessionException("session status is {}", newErSessionMeta.getStatus());
        }

        newErSessionMeta.getProcessors().forEach(p -> {
            if (contentMap.containsKey(p.getServerNodeId())) {
                contentMap.put(p.getTransferEndpoint().toString(), p.getServerNodeId());
                contentMap.remove(p.getServerNodeId());
            } else {
                log.info("download cannot found node {}", p.getServerNodeId());
            }
        });

        PrepareJobDownloadResponse response = new PrepareJobDownloadResponse();
        response.setSessionId(newErSessionMeta.getId());
        response.setContent(new Gson().toJson(contentMap));
        return response;
    }


}
