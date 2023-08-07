package com.webank.eggroll.clustermanager.cluster;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ServerNodeStatus;
import com.eggroll.core.constant.ServerNodeTypes;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.pojo.*;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class ClusterResourceManager {

    Logger log = LoggerFactory.getLogger(ClusterResourceManager.class);

    Map<String, ReentrantLock> sessionLockMap = new ConcurrentHashMap<>();
    Map<String, Long> killJobMap = new ConcurrentHashMap<>();
    FifoBroker<ResourceApplication> applicationQueue = new FifoBroker<>();

    @Autowired
    SessionMainService sessionMainService;
    @Autowired
    ServerNodeService serverNodeService;

    private Thread lockCleanThread = new Thread(() -> {
        while (true) {
            log.info("lock clean thread , prepare to run");
            long now = System.currentTimeMillis();
            sessionLockMap.forEach((k, v) -> {
                try {
                    ErSessionMeta es = sessionMainService.getSessionMain(k);
                    if (es.getUpdateTime() != null) {
                        long updateTime = es.getUpdateTime().getTime();
                        if (now - updateTime > MetaInfo.EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL
                                && (SessionStatus.KILLED.name().equals(es.getStatus())
                                || SessionStatus.ERROR.name().equals(es.getStatus())
                                || SessionStatus.CLOSED.name().equals(es.getStatus())
                                || SessionStatus.FINISHED.name().equals(es.getStatus()))) {
                            sessionLockMap.remove(es.getId());
                            killJobMap.remove(es.getId());
                        }
                    }
                } catch (Throwable e) {
                    log.error("lock clean error: " + e.getMessage());
                    // e.printStackTrace();
                }
            });
            try {
                Thread.sleep(MetaInfo.EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }, "LOCK-CLEAN-THREAD");

//    private Thread dispatchThread = new Thread(() -> {
//        log.info("resource dispatch thread start !!!");
//        while (true) {
//            ResourceApplication resourceApplication = null;
//            if (applicationQueue.getBroker().size() > 0) {
//                resourceApplication = applicationQueue.getBroker().peek();
//                log.info("resource application queue size {}", applicationQueue.getBroker().size());
//            } else {
//                try {
//                    Thread.sleep(MetaInfo.EGGROLL_RESOURCE_DISPATCH_INTERVAL);
//                } catch (Exception e) {
//                    log.error("Thread.sleep error");
//                }
//            }
//
//            try {
//                outerloop:
//                {
//                    if (resourceApplication != null) {
//                        long now = System.currentTimeMillis();
//                        List<ErServerNode> serverNodes;
//                        try {
//                            lockSession(resourceApplication.getSessionId());
//                            if (killJobMap.containsKey(resourceApplication.getSessionId())) {
//                                log.error("session " + resourceApplication.getSessionId() + " is already canceled, drop it");
//                                applicationQueue.getBroker().remove();
//                                break outerloop;
//                            }
//                            if (resourceApplication.getWaitingCount().get() == 0) {
//                                //过期资源申请
//                                log.error("expired resource request: " + resourceApplication + " !!!");
//                                applicationQueue.getBroker().remove();
//                                break outerloop;
//                            }
//                            int tryCount = 0;
//                            do {
//                                serverNodes = getServerNodeWithResource();
//                                tryCount += 1;
//                                if (serverNodes == null || serverNodes.size() == 0) {
//                                    try {
//                                        Thread.sleep(MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL);
//                                    } catch (InterruptedException e) {
//                                        e.printStackTrace();
//                                    }
//                                }
//                            } while ((serverNodes == null || serverNodes.size() == 0) && tryCount < 2);
//                            boolean enough = checkResourceEnough(serverNodes, resourceApplication);
//                            log.info("check resource is enough ? " + enough);
//                            if (!enough) {
//                                switch (resourceApplication.getResourceExhaustedStrategy()) {
//                                    case Dict.IGNORE:
//                                        break;
//                                    case Dict.WAITING:
//                                        Thread.sleep(MetaInfo.EGGROLL_RESOURCE_DISPATCH_INTERVAL);
//                                        log.info("resource is not enough, waiting next loop");
//                                        break outerloop;
//                                    case Dict.THROW_ERROR:
//                                        resourceApplication.getStatus().set(1);
//                                        resourceApplication.countDown();
//                                        applicationQueue.getBroker().remove();
//                                        break outerloop;
//                                }
//                            }
//                            switch (resourceApplication.getResourceExhaustedStrategy()) {
//                                case Dict.REMAIN_MOST_FIRST:
//                                    remainMostFirstDispatch(serverNodes, resourceApplication);
//                                    break;
//                                case Dict.RANDOM:
//                                    randomDispatch(serverNodes, resourceApplication);
//                                    break;
//                                case Dict.FIX:
//                                    fixDispatch(serverNodes, resourceApplication);
//                                    break;
//                                case Dict.SINGLE_NODE_FIRST:
//                                    singleNodeFirstDispatch(serverNodes, resourceApplication);
//                                    break;
//                            }
//                            ErSessionMeta[] dispatchedProcessors = resourceApplication.resourceDispatch;
//                            smDao.registerWithResource(new ErSessionMeta(
//                                    resourceApplication.sessionId,
//                                    resourceApplication.sessionName,
//                                    Arrays.stream(dispatchedProcessors).map(p -> p._1).toArray(ErProcessor[]::new),
//                                    dispatchedProcessors.length,
//                                    SessionStatus.NEW
//                            ));
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        } finally {
//                            unlockSession(resourceApplication.sessionId);
//                        }
//                        ErSessionMeta registeredSessionMeta = smDao.getSession(resourceApplication.sessionId);
//                        Map<String, ErServerNode> serverNodeMap = Arrays.stream(serverNodes)
//                                .collect(Collectors.toMap(ErServerNode::getId, Function.identity()));
//                        Tuple2<ErProcessor, ErServerNode>[] result = Arrays.stream(registeredSessionMeta.getProcessors())
//                                .map(p -> new Tuple2<>(p, serverNodeMap.get(p.getServerNodeId())))
//                                .toArray(Tuple2[]::new);
//                        resourceApplication.resourceDispatch.clear();
//                        Collections.addAll(resourceApplication.resourceDispatch, result);
//                        resourceApplication.countDown();
//                        applicationQueue.broker.remove();
//                    }
//                }
//            }
//
//        }
//    }, "RESOURCE_DISPATCH_THREAD");

    private List<ErServerNode> getServerNodeWithResource() {
        ErServerNode erServerNode = new ErServerNode();
        erServerNode.setStatus(ServerNodeStatus.HEALTHY.name());
        erServerNode.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        return serverNodeService.getListByErServerNode(erServerNode);
    }

    private Boolean checkResourceEnough(List<ErServerNode> erServerNodes, ResourceApplication resourceApplication) {
        boolean result = true;
        Map<String, Long> globalRemainResourceMap = new HashMap<>();
        Map<Long, Map<String, Long>> nodeRemainResourceMap = new HashMap<>();

        for (ErServerNode n : erServerNodes) {
            Map<String, Long> nodeMap = nodeRemainResourceMap.getOrDefault(n.getId(), new HashMap<>());
            for (ErResource r : n.getResources()) {
                long remain = nodeMap.getOrDefault(r.getResourceType(), 0L);
                long unAllocated = r.getUnAllocatedResource();
                nodeMap.put(r.getResourceType(), remain + unAllocated);
            }
            nodeRemainResourceMap.put(n.getId(), nodeMap);
        }

        nodeRemainResourceMap.forEach((k, v) -> v.forEach((str, lon) -> {
            Long count = globalRemainResourceMap.getOrDefault(str, 0L);
            globalRemainResourceMap.put(str, count + lon);
        }));

        if (!resourceApplication.isAllowExhausted()) {
            assert erServerNodes.size() > 0;
            if (Dict.FIX.equals(resourceApplication.getDispatchStrategy())) {
                int eggsPerNode = Integer.parseInt(resourceApplication.getOptions()
                        .getOrDefault(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE, MetaInfo.CONFKEY_SESSION_PROCESSORS_PER_NODE.toString()));
                String resourceType = resourceApplication.getOptions().getOrDefault("resourceType", Dict.VCPU_CORE);
                int types = resourceApplication.getProcessorTypes().size();
                result = nodeRemainResourceMap.entrySet().stream().allMatch(n -> {
                    long exist = n.getValue().getOrDefault(resourceType, 0L);
                    return exist >= eggsPerNode * types;
                });
            } else {
                List<ErProcessor> processors = resourceApplication.getProcessors();
//                    ErServerNode erServerNode = erServerNodes.stream().reduce((x, y) -> {
//                                x.getResources().addAll(y.getResources());
//                                return x;
//                            }
//                    ).orElse(null);
                Map<String, Long> requestResourceMap = new HashMap<>();

                ErProcessor erProcessor = processors.stream().reduce((x, y) -> {
                            x.getResources().addAll(y.getResources());
                            return x;
                        }
                ).orElse(null);

                if (erProcessor != null && erProcessor.getResources() != null) {
                    Map<String, List<ErResource>> collect = erProcessor.getResources().stream().collect(Collectors.groupingBy(ErResource::getResourceType));
                    collect.forEach((k, erResourceList) -> {
                        long sum = 0;
                        for (ErResource resource : erResourceList) {
                            sum += resource.getAllocated();
                        }
                        requestResourceMap.put(k, sum);
                    });

                }
                for (Map.Entry<String, Long> r : requestResourceMap.entrySet()) {
                    Long globalResourceRemain = globalRemainResourceMap.getOrDefault(r.getKey(), -1L);
                    if (globalResourceRemain.intValue() > -1) {
                        log.info("check resource " + r.getKey() + " request " + r.getValue() + " remain " + globalResourceRemain);
                        if (r.getValue() > globalResourceRemain) {
                            result = false;
                            break;
                        }
                    } else {
                        result = false;
                        break;
                    }
                }
            }
        }
        return result;
    }

    public void lockSession(String sessionId) {

    }
}
