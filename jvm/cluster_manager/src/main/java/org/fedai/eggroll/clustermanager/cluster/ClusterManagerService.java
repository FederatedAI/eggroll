package org.fedai.eggroll.clustermanager.cluster;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.clustermanager.dao.impl.NodeResourceService;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.clustermanager.entity.NodeResource;
import org.fedai.eggroll.clustermanager.entity.ServerNode;
import org.fedai.eggroll.clustermanager.job.JobServiceHandler;
import org.fedai.eggroll.clustermanager.session.SessionManager;
import org.fedai.eggroll.clustermanager.statemachine.ProcessorStateMachine;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.ProcessorStatus;
import org.fedai.eggroll.core.constant.SessionStatus;
import org.fedai.eggroll.core.constant.StatusReason;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.exceptions.ErSessionException;
import org.fedai.eggroll.core.grpc.NodeManagerClient;
import org.fedai.eggroll.core.pojo.*;
import org.fedai.eggroll.core.postprocessor.ApplicationStartedRunner;
import org.fedai.eggroll.core.utils.JsonUtil;
import org.fedai.eggroll.core.utils.LockUtils;
import org.mybatis.guice.transactional.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


@Singleton
public class ClusterManagerService implements ApplicationStartedRunner {

    Logger log = LoggerFactory.getLogger(ClusterManagerService.class);

    @Inject
    ServerNodeService serverNodeService;

    @Inject
    NodeResourceService nodeResourceService;

//    @Inject
//    SessionProcessorService sessionProcessorService;

    @Inject
    ProcessorStateMachine processorStateMachine;

    @Inject
    SessionMainService sessionMainService;

    @Inject
    JobServiceHandler jobServiceHandler;

    @Inject
    SessionManager sessionManager;

    Map<Long, ErNodeHeartbeat> nodeHeartbeatMap = new ConcurrentHashMap<>();
    public static Map<Long, ErProcessor> residualHeartbeatMap = new ConcurrentHashMap<>();
    Cache<Long, ReentrantLock> heartbeatLockCache = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL * 10, TimeUnit.MILLISECONDS)
            .build();

    public void addResidualHeartbeat(ErProcessor erProcessor) {
        residualHeartbeatMap.put(erProcessor.getId(), erProcessor);
    }

    public ErProcessor checkNodeProcess(Context context, ErEndpoint nodeManagerEndpoint, ErProcessor processor) {
        ErProcessor result = null;
        try {
            NodeManagerClient nodeManagerClient = new NodeManagerClient(nodeManagerEndpoint);
            result = nodeManagerClient.checkNodeProcess(context, processor);
        } catch (Exception e) {
            log.error("checkNodeProcess error :", e);
        }
        return result;
    }

    public void killResidualProcessor(Context context, ErProcessor processor) {
        log.info("prepare to kill redidual processor {}", JsonUtil.object2Json(processor));
        ErServerNode serverNodeInDb = serverNodeService.getByIdFromCache(processor.getServerNodeId());
        if (serverNodeInDb != null) {
            ErSessionMeta erSessionMeta = sessionMainService.getSession(processor.getSessionId(), true, false, false);
            if (erSessionMeta != null) {
                erSessionMeta.getOptions().put(Dict.SERVER_NODE_ID, processor.getServerNodeId().toString());
                NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNodeInDb.getEndpoint());
                nodeManagerClient.killContainers(context, erSessionMeta);
            }

        }
    }

    public void checkAndHandleDeepspeedOutTimeSession(Context context, ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        long current = System.currentTimeMillis();
        int maxInterval = MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS * 2;
        long interval = current - session.getCreateTime().getTime();
        log.debug("watch deepspeed new session: {} {}  {}", session.getId(), interval, maxInterval);
        if (interval > maxInterval) {
            jobServiceHandler.killJob(context, session.getId(), StatusReason.TIMEOUT.name());
        }
    }

    public void checkAndHandleEggpairOutTimeSession(ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        long current = System.currentTimeMillis();
        if (session.getCreateTime().getTime() < current - MetaInfo.EGGROLL_SESSION_STATUS_NEW_TIMEOUT_MS) {
            //session: ErSessionMeta, afterState: String
            log.info("session " + session + " status stay at " + session.getStatus() + " too long, prepare to kill");
            Context context = new Context();
            context.putData(Dict.STATUS_REASON,StatusReason.TIMEOUT.name());
            sessionManager.killSession(context, session.getId());
        }
    }

    public void checkAndHandleEggpairActiveSession(ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        Context context = new Context();
        long now = System.currentTimeMillis();
        long liveTime = MetaInfo.EGGROLL_SESSION_MAX_LIVE_MS;
        if (session.getCreateTime().getTime() < now - liveTime) {
            log.error("session " + session.getId() + " is timeout, live time is " + liveTime + ", max live time in config is " + MetaInfo.EGGROLL_SESSION_MAX_LIVE_MS);
            context.putData(Dict.STATUS_REASON,StatusReason.TIMEOUT.name());
            sessionManager.killSession(context, session, SessionStatus.ERROR.name());
        } else {
            List<ErProcessor> invalidProcessor = sessionProcessors.stream().filter(p -> StringUtils.equalsAny(p.getStatus(),
                    ProcessorStatus.ERROR.name(), ProcessorStatus.KILLED.name(), ProcessorStatus.STOPPED.name())).collect(Collectors.toList());
            if (invalidProcessor.size() > 0) {
                boolean needKillSession = invalidProcessor.stream().anyMatch(p -> p.getUpdatedAt().getTime() < now - MetaInfo.EGGROLL_SESSION_STOP_TIMEOUT_MS);
                if (needKillSession) {
                    log.info("invalid processors " + JsonUtil.object2Json(invalidProcessor) + ", session watcher kill eggpair session " + session);
                    context.putData(Dict.STATUS_REASON,StatusReason.PROCESS_ERROR.name());
                    sessionManager.killSession(new Context(), session, SessionStatus.ERROR.name());
                }
            }
        }
    }

    public void checkAndHandleDeepspeedActiveSession(Context context, ErSessionMeta session, List<ErProcessor> sessionProcessors) {
        log.info("checkAndHandleDeepspeedActiveSession " + session.getId() + " " + JsonUtil.object2Json(sessionProcessors));

        if (sessionProcessors.stream().anyMatch(p -> ProcessorStatus.ERROR.name().equals(p.getStatus()))) {
            log.info("session watcher kill session " + session);
            try {
                jobServiceHandler.killJob(context, session.getId(), StatusReason.PROCESS_ERROR.name());
            } catch (ErSessionException e) {
                log.error("failed to kill session " + session.getId(), e);
            }
        } else if (sessionProcessors.stream().allMatch(p -> ProcessorStatus.FINISHED.name().equals(p.getStatus()))) {
            session.setStatus(SessionStatus.FINISHED.name());
            sessionMainService.updateSessionMain(session, erSessionMeta -> erSessionMeta.getProcessors().forEach(processor -> processorStateMachine.changeStatus(new Context(), processor, null, erSessionMeta.getStatus())));
        }
        log.debug("found all processor belongs to session " + session.getId() + " finished, update session status to `Finished`");
    }

    public ErNodeHeartbeat nodeHeartbeat(Context context, ErNodeHeartbeat nodeHeartbeat) {
        ErServerNode serverNode = nodeHeartbeat.getNode();
        LockUtils.lock(heartbeatLockCache, serverNode.getId());
        try {
            if (!Dict.LOSS.equals(serverNode.getStatus())) {
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
                        ErServerNode existsServerNode = serverNodeService.getByIdFromCache(serverNode.getId());
                        if (existsServerNode == null) {
                            serverNode = createNewNode(serverNode);
                        } else {
                            updateNode(serverNode, true, true);
                        }
                    }
                }
            } else {
                if (serverNode.getId() != -1) {
                    log.info("receive node {} quit heart beat", serverNode.getId());
                    updateNode(serverNode, false, true);
                }
            }
            nodeHeartbeatMap.put(serverNode.getId(), nodeHeartbeat);
            nodeHeartbeat.setNode(serverNode);

        } finally {
            LockUtils.unLock(heartbeatLockCache, serverNode.getId());
        }
        return nodeHeartbeat;
    }

    @Transactional
    public ErServerNode updateNode(ErServerNode serverNode, Boolean needUpdateResource, Boolean isHeartbeat) {
        serverNodeService.updateByErNode(serverNode, isHeartbeat);
        if (needUpdateResource) {
            registerResource(serverNode);
        }
        return serverNode;
    }

    @Transactional
    public ErServerNode registerResource(ErServerNode data) {
        log.info("node {} register resource {}", data.getId(), JsonUtil.object2Json(data.getResources()));
        NodeResource nodeResource = new NodeResource();
        nodeResource.setServerNodeId(data.getId());
        List<NodeResource> nodeResourceList = nodeResourceService.list(nodeResource);
        List<ErResource> existResources = new ArrayList<>();
        for (NodeResource resource : nodeResourceList) {
            existResources.add(resource.toErResource());
        }
        List<ErResource> registedResources = data.getResources();
        List<ErResource> updateResources = new ArrayList<>();
        List<ErResource> deleteResources = new ArrayList<>();
        List<ErResource> insertResources = new ArrayList<>();
        for (ErResource e : existResources) {
            boolean needUpdate = false;
            for (ErResource updatedResource : registedResources) {
                if (updatedResource.getResourceType().equals(e.getResourceType())) {
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


    @Override
    public void run(String[] args) {
        //delete cluster manager
        ServerNode cluster = new ServerNode();
        cluster.setHost(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST);
        cluster.setPort(MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT);
        cluster.setNodeType(Dict.CLUSTER_MANAGER);
        serverNodeService.remove(new QueryWrapper<>(cluster));

        //insert clustermanager
        cluster.setStatus(Dict.HEALTHY);
        cluster.setServerClusterId(0L);
        cluster.setLastHeartbeatAt(new Date());
        serverNodeService.save(cluster);
    }
}
