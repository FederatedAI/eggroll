package com.webank.eggroll.clustermanager.cluster;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ServerNodeStatus;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.exceptions.ErSessionException;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.NodeResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import com.webank.eggroll.clustermanager.job.JobServiceHandler;
import com.webank.eggroll.clustermanager.session.SessionManager;
import com.webank.eggroll.clustermanager.statemachine.ProcessorStateMachine;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


@Service
public class ClusterManagerService implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    ServerNodeService serverNodeService;
    @Autowired
    NodeResourceService nodeResourceService;
    @Autowired
    SessionProcessorService sessionProcessorService;
    @Autowired
    ProcessorStateMachine processorStateMachine;
    @Autowired
    SessionMainService sessionMainService;
    @Autowired
    JobServiceHandler jobServiceHandler;
    @Autowired
    SessionManager sessionManager;
    @Autowired
    ClusterResourceManager clusterResourceManager;

    Logger log = LoggerFactory.getLogger(ClusterManagerService.class);

    Map<Long, ErNodeHeartbeat> nodeHeartbeatMap = new ConcurrentHashMap<>();
    Map<Long, ErProcessor> residualHeartbeatMap = new ConcurrentHashMap<>();

    public ErProcessor checkNodeProcess(ErEndpoint nodeManagerEndpoint, ErProcessor processor) {
        ErProcessor result = null;
        try {
            NodeManagerClient nodeManagerClient = new NodeManagerClient(nodeManagerEndpoint);
            result = nodeManagerClient.checkNodeProcess(processor);
        } catch (Exception e) {
            log.error("checkNodeProcess error :", e);
        }
        return result;
    }

    public void checkDbRunningProcessor() {
        try {
            long now = System.currentTimeMillis();
            ErProcessor erProcessor = new ErProcessor();
            erProcessor.setStatus(ProcessorStatus.RUNNING.name());
            List<ErProcessor> erProcessors = sessionProcessorService.doQueryProcessor(erProcessor);

            Map<Long, List<ErProcessor>> grouped = erProcessors.stream().collect(Collectors.groupingBy(ErProcessor::getServerNodeId));

            grouped.forEach((serverNodeId, processorList) -> {
                ServerNode serverNode = serverNodeService.getById(serverNodeId);
                NodeManagerClient nodeManagerClient = new NodeManagerClient(new ErEndpoint(serverNode.getHost(), serverNode.getPort()));
                for (ErProcessor processor : processorList) {
                    ErProcessor result = nodeManagerClient.checkNodeProcess(processor);
                    if (result == null || ProcessorStatus.KILLED.name().equals(result.getStatus())) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        SessionProcessor processorInDb = sessionProcessorService.getById(processor.getId());
                        if (processorInDb != null) {
                            if (ProcessorStatus.RUNNING.name().equals(processorInDb.getStatus())) {
                                ErProcessor checkNodeProcessResult = nodeManagerClient.checkNodeProcess(processor);
                                if (checkNodeProcessResult == null || ProcessorStatus.KILLED.name().equals(checkNodeProcessResult.getStatus())) {
                                    processorStateMachine.changeStatus(new Context(), processor, null, ProcessorStatus.ERROR.name());
                                }
                            }

                        }
                    }
                }
            });
        } catch (Exception e) {
            log.error("checkDbRunningProcessor error :", e);
        }
    }

    public void killResidualProcessor(ErProcessor processor) {
        log.info("prepare to kill redidual processor {}", JsonUtil.object2Json(processor));
        ErServerNode serverNode = serverNodeService.getById(processor.getId()).toErServerNode();
        ErSessionMeta erSessionMeta = sessionMainService.getSession(processor.getSessionId());
        erSessionMeta.getOptions().put(MetaInfo.SERVER_NODE_ID, processor.getServerNodeId().toString());
        NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNode.getEndpoint());
        nodeManagerClient.killContainers(erSessionMeta);
    }

    Thread redidualProcessorChecker = new Thread(() -> {
        while (true) {
            try {
                residualHeartbeatMap.forEach((k, v) -> {
                    try {
                        killResidualProcessor(v);
                        residualHeartbeatMap.remove(k);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        log.error("kill residual processor error: " + e.getMessage());
                    }
                });
            } catch (Throwable e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }, "REDIDUAL_PROCESS_CHECK_THREAD");

    Thread nodeProcessChecker = new Thread(new Runnable() {
        @Override
        public void run() {
            while (true) {
                try {
                    checkDbRunningProcessor();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }
    }, "NODE_PROCESS_CHECK_THREAD");

    public void checkAndHandleDeepspeedOutTimeSession(ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        long current = System.currentTimeMillis();
        Integer maxInterval = MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS * 2;
        long interval = current - session.getCreateTime().getTime();
        log.debug("watch deepspeed new session: {} {}  {}", session.getId(), interval, maxInterval);
        if (interval > maxInterval) {
            jobServiceHandler.killJob(session.getId());
        }
    }

    public void checkAndHandleEggpairOutTimeSession(ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        long current = System.currentTimeMillis();
        if (session.getCreateTime().getTime() < current - MetaInfo.EGGROLL_SESSION_STATUS_NEW_TIMEOUT_MS) {
            //session: ErSessionMeta, afterState: String
            log.info("session " + session + " status stay at " + session.getStatus() + " too long, prepare to kill");
            sessionManager.killSession(null, session.getId());
        }
    }

    public void checkAndHandleEggpairActiveSession(ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        long now = System.currentTimeMillis();
        long liveTime = MetaInfo.EGGROLL_SESSION_MAX_LIVE_MS;
        if (session.getCreateTime().getTime() < now - liveTime) {
            log.error("session " + session.getId() + " is timeout, live time is " + liveTime + ", max live time in config is " + MetaInfo.EGGROLL_SESSION_MAX_LIVE_MS);
            sessionManager.killSession(null, session);
        } else {
            List<ErProcessor> invalidProcessor = sessionProcessors.stream().filter(p -> StringUtils.equalsAny(p.getStatus(),
                    ProcessorStatus.ERROR.name(), ProcessorStatus.KILLED.name(), ProcessorStatus.STOPPED.name())).collect(Collectors.toList());
            if (invalidProcessor.size() > 0) {
                boolean needKillSession = invalidProcessor.stream().anyMatch(p -> p.getUpdatedAt().getTime() < now - MetaInfo.EGGROLL_SESSION_STOP_TIMEOUT_MS);
                if (needKillSession) {
                    log.info("invalid processors " + JsonUtil.object2Json(invalidProcessor) + ", session watcher kill eggpair session " + session);
                    sessionManager.killSession(null, session);
                }
            }
        }
    }

    public void checkAndHandleDeepspeedActiveSession(ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        log.info("checkAndHandleDeepspeedActiveSession " + session.getId() + " " + JsonUtil.object2Json(sessionProcessors));

        if (sessionProcessors.stream().anyMatch(p -> ProcessorStatus.ERROR.name().equals(p.getStatus()))) {
            log.info("session watcher kill session " + session);
            try {
                jobServiceHandler.killJob(session.getId());
            } catch (ErSessionException e) {
                log.error("failed to kill session " + session.getId(), e);
            }
        } else if (sessionProcessors.stream().anyMatch(p -> ProcessorStatus.FINISHED.name().equals(p.getStatus()))) {
            session.setStatus(SessionStatus.FINISHED.name());
            sessionMainService.updateSessionMain(session, erSessionMeta -> erSessionMeta.getProcessors().forEach(processor -> processorStateMachine.changeStatus(new Context(), processor, null, erSessionMeta.getStatus())));
        }
        log.debug("found all processor belongs to session " + session.getId() + " finished, update session status to `Finished`");
    }

    private final Thread sessionWatcher = new Thread(() -> {
        while (true) {
            try {
                List<ErSessionMeta> sessions = sessionMainService.getSessionMainsByStatus(Arrays.asList(SessionStatus.ACTIVE.name(), SessionStatus.NEW.name()));

                for (ErSessionMeta session : sessions) {
                    try {
                        List<ErProcessor> sessionProcessors = sessionMainService.getSession(session.getId()).getProcessors();
                        String ACTIVE = SessionStatus.ACTIVE.name();
                        String NEW = SessionStatus.NEW.name();

                        switch (session.getName()) {
                            case "DeepSpeed":
                                log.debug("watch deepspeed session: " + session.getId() + " " + session.getStatus());
                                if (SessionStatus.ACTIVE.name().equals(session.getStatus())) {
                                    checkAndHandleDeepspeedActiveSession(session, sessionProcessors);
                                } else if (SessionStatus.NEW.name().equals(session.getStatus())) {
                                    checkAndHandleDeepspeedOutTimeSession(session, sessionProcessors);
                                }
                                break;
                            default:
                                if (SessionStatus.ACTIVE.name().equals(session.getStatus())) {
                                    checkAndHandleEggpairActiveSession(session, sessionProcessors);
                                } else if (SessionStatus.NEW.name().equals(session.getStatus())) {
                                    checkAndHandleEggpairOutTimeSession(session, sessionProcessors);
                                }
                                break;
                        }
                    } catch (Throwable e) {
                        log.error("session watcher handle session " + session.getId() + " error " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                Thread.sleep(MetaInfo.EGGROLL_SESSION_STATUS_CHECK_INTERVAL_MS);
            } catch (Throwable e) {
                log.error("session watcher handle error ", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }
        }
    }, "SESSION_WATCHER_THREAD");

    private final Thread nodeHeartbeatChecker = new Thread(() -> {
        long expire = MetaInfo.CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT *
                MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL;

        while (true) {
            try {
                long now = System.currentTimeMillis();
                ErServerNode erServerNode = new ErServerNode();
                erServerNode.setStatus(ServerNodeStatus.HEALTHY.name());
                List<ErServerNode> nodes = serverNodeService.getListByErServerNode(erServerNode);

                for (ErServerNode node : nodes) {
                    long interval = now - (node.getLastHeartBeat() != null ?
                            node.getLastHeartBeat().getTime() : now);
                    if (interval > expire) {
                        log.info("server node " + node + " change status to LOSS");
                        node.setStatus(ServerNodeStatus.LOSS.name());
                        updateNode(node, false, false);
                    }
                }
            } catch (Throwable e) {
                log.error("handle node heart beat error: ", e);
            }

            try {
                Thread.sleep(expire + 1000);
            } catch (InterruptedException e) {
                log.error("node heartbeat checker thread interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }, "NODE_HEART_BEAT_CHECK_THREAD");

//    private void killJob(String sessionId, boolean isTimeout) {
//        log.info("killing job " + sessionId);
//        try {
//            clusterResourceManager.lockSession(sessionId);
//            clusterResourceManager.getKillJobMap().put(sessionId, System.currentTimeMillis());
//            if (sessionMainService.getById(sessionId) == null) {
//                return;
//            }
//            ErSessionMeta sessionMeta = sessionMainService.getSession(sessionId);
//            if (StringUtils.equalsAny(sessionMeta.getStatus(), SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name())) {
//                return;
//            }
//            Map<Long, List<ErProcessor>> serverIdErProcessorsMap = sessionMeta.getProcessors()
//                    .stream().collect(Collectors.groupingBy(ErProcessor::getServerNodeId));
//
//            Map<ErServerNode,List<ErProcessor>> nodeAndProcessors = new HashMap<>();
//            serverIdErProcessorsMap.forEach((nodeId, processors) -> {
//                ServerNode serverNode = serverNodeService.getById(nodeId);
//                if(serverNode!=null){
//                    nodeAndProcessors.put(serverNode.toErServerNode(),processors);
//                }
//            });
//            nodeAndProcessors.forEach((node, processors)->{
//                KillContainersRequest killContainersRequest = new KillContainersRequest();
//                killContainersRequest.setSessionId(sessionId);
//                List<Long> processorIdList = new ArrayList<>();
//                for (ErProcessor processor : processors) {
//                    processorIdList.add(processor.getId());
//                }
//                try {
//                    killContainersRequest.setContainers(processorIdList);
//                    new NodeManagerClient(node.getEndpoint()).killJobContainers(killContainersRequest);
//                } catch (Exception e) {
//                    log.error("killContainers error : ", e);
//                }
//            });
//            smDao.updateSessionMain(sessionMeta.copy(status = SessionStatus.ERROR), defaultSessionCallback);
//        } finally {
//            ClusterResourceManager.unlockSession(sessionId);
//        }
//    }


    public ErNodeHeartbeat nodeHeartbeat(ErNodeHeartbeat nodeHeartbeat) {
        ErServerNode serverNode = nodeHeartbeat.getNode();
        synchronized (serverNode.getId().toString().intern()) {
            if (serverNode.getId() == -1) {
                ServerNode existNode = serverNodeService.getByEndPoint(serverNode.getEndpoint());
                if (existNode == null) {
                    log.info("create new node {}", JsonUtil.object2Json(serverNode));
                    serverNodeService.createByErNode(serverNode);
                } else {
                    log.info("node already exist {}", existNode);
                    serverNode.setId(existNode.getServerNodeId());
                    updateNode(serverNode, true, true);
                }
            } else {
                if (nodeHeartbeatMap.containsKey(serverNode.getId())
                        && (nodeHeartbeatMap.get(serverNode.getId()).getId() < nodeHeartbeat.getId())) {
                    //正常心跳
                    updateNode(serverNode, false, true);
                } else {
                    //nodemanger重启过
                    ServerNode existsServerNode = serverNodeService.getById(serverNode.getId());
                    if (existsServerNode == null) {
                        serverNode = createNewNode(serverNode);
                    } else {
                        updateNode(serverNode, true, true);
                    }
                }
            }
            nodeHeartbeatMap.put(serverNode.getId(), nodeHeartbeat);
            nodeHeartbeat.setNode(serverNode);
        }
        return nodeHeartbeat;
    }

    public ErServerNode updateNode(ErServerNode serverNode, Boolean needUpdateResource, Boolean isHeartbeat) {
        serverNodeService.updateByErNode(serverNode, isHeartbeat);
        if (needUpdateResource) {
            registerResource(serverNode);
        }
        return serverNode;
    }

    public ErServerNode registerResource(ErServerNode data) {
        log.info("node {} register resource {}", data.getId(), JsonUtil.object2Json(data.getResources()));
        NodeResource nodeResource = new NodeResource();
        nodeResource.setServerNodeId(data.getId());
        List<NodeResource> nodeResourceList = nodeResourceService.list(nodeResource);
        List<ErResource> existResources = new ArrayList<>();
        for (NodeResource resource : nodeResourceList) {
            ErResource erResource = new ErResource();
            BeanUtils.copyProperties(resource, erResource);
            existResources.add(erResource);
        }
        List<ErResource> registedResources = data.getResources();
        List<ErResource> updateResources = new ArrayList<>();
        List<ErResource> deleteResources = new ArrayList<>();
        List<ErResource> insertResources = new ArrayList<>();
        for (ErResource e : existResources) {
            boolean needUpdate = false;
            for (ErResource r : registedResources) {
                if (r.getResourceType().equals(e.getResourceType())) {
                    ErResource updatedResource = new ErResource();
                    BeanUtils.copyProperties(r, updatedResource);
                    updatedResource.setAllocated(-1L);
                    needUpdate = true;
                    updateResources.add(updatedResource);
                }
            }
            if (!needUpdate) {
                deleteResources.add(e);
            }
        }

        for (ErResource r : registedResources) {
            if (!updateResources.contains(r)) {
                insertResources.add(r);
            }
        }

        nodeResourceService.registerResource(data.getId(), insertResources, updateResources, deleteResources);
        return data;
    }

    public ErServerNode createNewNode(ErServerNode serverNode) {
        ServerNode existNode = serverNodeService.createByErNode(serverNode);
        serverNode.setId(existNode.getServerNodeId());
        return registerResource(serverNode);
    }


    public void onApplicationEvent(@NotNull ApplicationReadyEvent   event) {
        sessionWatcher.start();
        nodeHeartbeatChecker.start();
        nodeProcessChecker.start();
        redidualProcessorChecker.start();
    }
}
