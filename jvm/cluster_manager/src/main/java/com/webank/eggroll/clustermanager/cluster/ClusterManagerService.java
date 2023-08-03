package com.webank.eggroll.clustermanager.cluster;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.context.Context;
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
import com.webank.eggroll.clustermanager.statemechine.ProcessorStateMechine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


@Service
public class ClusterManagerService {

    @Autowired
    ServerNodeService serverNodeService;
    @Autowired
    NodeResourceService nodeResourceService;
    @Autowired
    SessionProcessorService sessionProcessorService;
    @Autowired
    ProcessorStateMechine processorStateMechine;
    @Autowired
    SessionMainService sessionMainService;

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
                                    processorStateMechine.changeStatus(new Context(), processor, null, ProcessorStatus.ERROR.name());
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

    private Thread residualProcessorChecker = new Thread(new Runnable() {
        @Override
        public void run() {
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
        }
    }, "REDIDUAL_PROCESS_CHECK_THREAD");

    private Thread nodeProcessChecker = new Thread(new Runnable() {
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
        if(interval > maxInterval){

        }
    }


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
                    updatedResource.setAllocated(-1);
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
}
