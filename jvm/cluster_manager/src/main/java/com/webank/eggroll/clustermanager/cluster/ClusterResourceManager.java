package com.webank.eggroll.clustermanager.cluster;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.*;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.ProcessorResource;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;


import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class ClusterResourceManager  implements ApplicationListener<ApplicationReadyEvent> {

    Logger log = LoggerFactory.getLogger(ClusterResourceManager.class);

    Map<String, ReentrantLock> sessionLockMap = new ConcurrentHashMap<>();
    Map<String, Long> killJobMap = new ConcurrentHashMap<>();
    FifoBroker<ResourceApplication> applicationQueue = new FifoBroker<>();

    @Autowired
    SessionMainService sessionMainService;
    @Autowired
    ServerNodeService serverNodeService;

    @Autowired
    NodeResourceService  nodeResourceService;
    @Autowired
    ProcessorResourceService  processorResourceService;


    BlockingQueue<Long> nodeResourceUpdateQueue =   new ArrayBlockingQueue(100);

    public  void countAndUpdateNodeResource(Long  serverNodeId){
        this.nodeResourceUpdateQueue.add(serverNodeId);
    }

    private  void   countAndUpdateNodeResourceInner(Long serverNodeId){

        List<ProcessorResource> resourceList = this.processorResourceService.list( new LambdaQueryWrapper<ProcessorResource>()
                .eq(ProcessorResource::getServerNodeId, serverNodeId).in(ProcessorResource::getResourceType, Lists.newArrayList(ResourceStatus.ALLOCATED.getValue() ,ResourceStatus.PRE_ALLOCATED.getValue())));
        List<ErResource>  prepareUpdateResource = Lists.newArrayList();
        if(resourceList!=null){
            Map<String,ErResource> resourceMap = Maps.newHashMap();
            resourceList.forEach(processorResource->{
                String status = processorResource.getStatus();
                ErResource nodeResource = resourceMap.get(processorResource.getResourceType());
                if(nodeResource==null){
                    resourceMap.put(processorResource.getResourceType(),new ErResource());
                    nodeResource = resourceMap.get(processorResource.getResourceType());
                }
                if(status.equals(ResourceStatus.ALLOCATED.getValue())){
                    if(nodeResource.getAllocated()!=null)
                        nodeResource.setAllocated(nodeResource.getAllocated()+processorResource.getAllocated());
                    else
                        nodeResource.setAllocated(processorResource.getAllocated());

                }else{
                    if(nodeResource.getPreAllocated()!=null)
                        nodeResource.setPreAllocated(nodeResource.getPreAllocated()+processorResource.getAllocated());
                    else
                        nodeResource.setPreAllocated(processorResource.getAllocated());
                }
            });
            prepareUpdateResource.addAll(resourceMap.values());
        }else{
            ErResource gpuResource = new ErResource();
            gpuResource.setServerNodeId(serverNodeId);
            gpuResource.setResourceType(ResourceType.VGPU_CORE.name());
            gpuResource.setAllocated(0L);
            gpuResource.setPreAllocated(0L);
            ErResource  cpuResource = new ErResource();
            cpuResource.setServerNodeId(serverNodeId);
            cpuResource.setResourceType(ResourceType.VCPU_CORE.name());
            cpuResource.setAllocated(0L);
            cpuResource.setPreAllocated(0L);
            prepareUpdateResource.add(gpuResource);
            prepareUpdateResource.add(cpuResource);
        }
        nodeResourceService.doUpdateNodeResource(serverNodeId,prepareUpdateResource);
    }


    private Thread  countNodeResourceThread =  new  Thread(()->{
        while(true){
            try{
                List<Long>   nodeList = Lists.newArrayList();
                nodeResourceUpdateQueue.drainTo(nodeList);
                Set<Long>  nodeSet = new HashSet<>();
                nodeSet.addAll(nodeList);
                for(Long nodeId: nodeSet){
                    countAndUpdateNodeResourceInner(nodeId);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    },"RESOURCE-COUNT-THREAD");







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
//                            List<Map<ErProcessor, ErServerNode>> dispatchedProcessors = resourceApplication.getResourceDispatch();
//                            smDao.registerWithResource(new ErSessionMeta(
//                                    resourceApplication.getSessionId(),
//                                    resourceApplication.getSessionName(),
//                                    Arrays.stream(dispatchedProcessors).map(p -> p._1).toArray(ErProcessor[]::new),
//                                    dispatchedProcessors.size(),
//                                    SessionStatus.NEW
//                            ));
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        } finally {
//                            unlockSession(resourceApplication.getSessionId());
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

    public ResourceApplication remainMostFirstDispatch(List<ErServerNode> serverNodes, ResourceApplication resourceApplication) {
        List<ErProcessor> requiredProcessors = resourceApplication.getProcessors();
        List<ErServerNode> sortedNodes = serverNodes.stream().sorted(Comparator.comparingLong(node -> getFirstUnAllocatedResource(node, resourceApplication))).collect(Collectors.toList());
        Map<ErServerNode, Long> sortMap = new HashMap<>();
        for (ErServerNode node : sortedNodes) {
            sortMap.put(node, getFirstUnAllocatedResource(node, resourceApplication));
        }
        List<String> allocatedGpuIndex = new ArrayList<>();

        Map<ErServerNode, List<ErProcessor>> nodeToProcessors = new HashMap<>();

        for (int index = 0; index < requiredProcessors.size(); index++) {
            ErProcessor requiredProcessor = requiredProcessors.get(index);
            ErServerNode node = sortedNodes.get(index);

            int nextGpuIndex = -1;
            List<ErResource> newResources = new ArrayList<>();
            for (ErResource r : requiredProcessor.getResources()) {
                ErResource changedResource = r;
                if (Dict.VGPU_CORE.equals(r.getResourceType())) {
                    List<ErResource> gpuResourcesInNodeArray = node.getResources().stream()
                            .filter(res -> Dict.VGPU_CORE.equals(res.getResourceType()))
                            .collect(Collectors.toList());
                    if (!gpuResourcesInNodeArray.isEmpty()) {
                        ErResource gpuResourcesInNode = gpuResourcesInNodeArray.get(0);

                        List<String> extentionCache = Arrays.asList(gpuResourcesInNode.getExtention().split(","));
                        nextGpuIndex = getNextGpuIndex(gpuResourcesInNode.getTotal(), extentionCache);
                        extentionCache.add(String.valueOf(nextGpuIndex));
                        changedResource.setExtention(String.valueOf(nextGpuIndex));
                    }
                }
                newResources.add(changedResource);
            }
            String host = node.getEndpoint().getHost();
            requiredProcessor.setServerNodeId(node.getId());
            requiredProcessor.setCommandEndpoint(new ErEndpoint(host, 0));
            requiredProcessor.setResources(newResources);
            Map<String, String> optionsMap = new HashMap<>();
            optionsMap.put("cudaVisibleDevices", nextGpuIndex + "");
            requiredProcessor.setOptions(optionsMap);
            nodeToProcessors.computeIfAbsent(node, k -> new ArrayList<>()).add(requiredProcessor);
        }

        Map<ErProcessor, ErServerNode> result = new HashMap<>();
        nodeToProcessors.forEach((node, processors) -> {
            for (ErProcessor processor : processors) {
                result.put(processor, node);
            }
        });

        resourceApplication.getResourceDispatch().add(result);
        return resourceApplication;

    }

    private Long getFirstUnAllocatedResource(ErServerNode serverNode, ResourceApplication resourceApplication) {
        for (ErResource resource : serverNode.getResources()) {
            if (resource.getResourceType().equals(resourceApplication.getSortByResourceType())) {
                return resource.getUnAllocatedResource();
            }
        }
        return 0L;
    }

    private int getNextGpuIndex(Long size, List<String> alreadyAllocated) {
        int result = -1;

        for (int index = 0; index < size; index++) {
            boolean isAllocated = false;
            for (String allocated : alreadyAllocated) {
                if (allocated.equals(String.valueOf(index))) {
                    isAllocated = true;
                    break;
                }
            }
            if (!isAllocated) {
                result = index;
                break;
            }
        }

        log.info("get next gpu index, size: " + size + " alreadyAllocated: " + JsonUtil.object2Json(alreadyAllocated) + " return: " + result);
        return result;
    }

    private static ResourceApplication randomDispatch(List<ErServerNode> serverNodes, ResourceApplication resourceApplication) {
        List<ErProcessor> requiredProcessors = resourceApplication.getProcessors();
        List<ErServerNode> shuffledNodes = new ArrayList<>(serverNodes);
        Collections.shuffle(shuffledNodes);
        Map<ErServerNode, List<ErProcessor>> nodeToProcessors = new HashMap<>();

        for (int index = 0; index < requiredProcessors.size(); index++) {
            ErProcessor requiredProcessor = resourceApplication.getProcessors().get(index);
            ErServerNode node = shuffledNodes.get(0);

            String host = node.getEndpoint().getHost();
            int globalRank = index;
            int localRank = nodeToProcessors.getOrDefault(node, new ArrayList<>()).size();
            requiredProcessor.setServerNodeId(node.getId());
            requiredProcessor.setCommandEndpoint(new ErEndpoint(host, 0));
            if (nodeToProcessors.containsKey(node)) {
                nodeToProcessors.get(node).add(requiredProcessor);
            } else {
                nodeToProcessors.put(node, new ArrayList<>(Collections.singletonList(requiredProcessor)));
            }

            shuffledNodes.remove(0);
        }

        Map<ErProcessor, ErServerNode> result = new HashMap<>();
        for (Map.Entry<ErServerNode, List<ErProcessor>> entry : nodeToProcessors.entrySet()) {
            ErServerNode node = entry.getKey();
            List<ErProcessor> processors = entry.getValue();

            for (ErProcessor processor : processors) {
                result.put(processor, node);
            }
        }

        resourceApplication.getResourceDispatch().add(result);
        return resourceApplication;
    }


    public void lockSession(String sessionId) {

    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {

    }
}
