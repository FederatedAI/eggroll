package org.fedai.eggroll.nodemanager.service;

import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErNodeHeartbeat;
import org.fedai.eggroll.core.pojo.ErResource;
import org.fedai.eggroll.core.pojo.ErServerNode;
import org.fedai.eggroll.core.postprocessor.ApplicationStartedRunner;
import org.fedai.eggroll.core.utils.NetUtils;
import com.google.inject.Singleton;
import org.fedai.eggroll.nodemanager.env.SysInfoLinux;
import org.fedai.eggroll.nodemanager.meta.NodeManagerMeta;
import org.fedai.eggroll.nodemanager.pojo.ResourceWrapper;
import org.fedai.eggroll.nodemanager.schedule.NodeManagerTask;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class NodeResourceManager implements ApplicationStartedRunner {

    Logger logger = LoggerFactory.getLogger(NodeResourceManager.class);

    //    @Inject
    private final SysInfoLinux sysInfo;

    ClusterManagerClient client;
    Long physicalMemorySize;
    HeartBeatThread heartBeatThread;
    ResourceCountThread resourceCountThread;
    Map<String, ResourceWrapper> resourceMap;

    Long seq = 0L;

    public NodeResourceManager() {
        sysInfo = new SysInfoLinux();
        int cpus = getAvailableProcessors();
        int gpus = getGpuSize();
        ResourceWrapper cpuCore = new ResourceWrapper(Dict.VCPU_CORE, new AtomicLong(cpus));
        ResourceWrapper physicalMemory = new ResourceWrapper(Dict.PHYSICAL_MEMORY, new AtomicLong(getPhysicalMemorySize()));
        ResourceWrapper gpuCore = new ResourceWrapper(Dict.VGPU_CORE, new AtomicLong(gpus));
        resourceMap = new HashMap<>();
        resourceMap.put(Dict.VCPU_CORE, cpuCore);
        resourceMap.put(Dict.PHYSICAL_MEMORY, physicalMemory);
        resourceMap.put(Dict.VGPU_CORE, gpuCore);
        client = new ClusterManagerClient(new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST, MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));
        physicalMemorySize = getPhysicalMemorySize();
        heartBeatThread = new HeartBeatThread();
        resourceCountThread = new ResourceCountThread();
    }

    public ResourceWrapper getResourceWrapper(String rType) {
        return resourceMap.get(rType);
    }

    public Boolean checkResourceIsEnough(String rType, Long required) {
        ResourceWrapper resourceWrapper = getResourceWrapper(rType);
        logger.info("checkResourceIsEnough {} {}", rType, required);
        if (resourceWrapper != null) {
            long left = resourceWrapper.getTotal().get() - resourceWrapper.getAllocated().get();
            return required <= left;
        }
        return false;
    }

    public Long freeResource(String rType, Long count) {
        if (count < 0) {
            logger.error("The parameter must be greater than 0");
            return 0L;
        }
        ResourceWrapper resourceWrapper = getResourceWrapper(rType);
        return resourceWrapper.getAllocated().getAndAdd(-count);
    }

    public Long allocateResource(String rType, Long count) {
        if (count < 0) {
            logger.error("The parameter must be greater than 0");
            return 0L;
        }
        ResourceWrapper resourceWrapper = getResourceWrapper(rType);
        return resourceWrapper.getAllocated().getAndAdd(count);
    }

    public Long getPhysicalMemorySize() {
        return sysInfo.getPhysicalMemorySize();
    }

    public Integer getGpuSize() {
        int defaultSize = 0;
        try {
            defaultSize = sysInfo.getGpuNumber();
        } catch (IOException e) {
            logger.error("get gpuSize failed: {}", e.getMessage());
        }
        return defaultSize;
    }

    public Long getAvailablePhysicalMemorySize() {
        return sysInfo.getAvailablePhysicalMemorySize();
    }

    public int getAvailableProcessors() {
        return sysInfo.getNumCores();
    }

    public void countMemoryResource() {
        Long available = sysInfo.getAvailablePhysicalMemorySize();
        resourceMap.get(Dict.PHYSICAL_MEMORY).getUsed().set(physicalMemorySize - available);
    }

    public void countCpuResource() {
        int coreUsed = (int) sysInfo.getNumVCoresUsed();
        resourceMap.get(Dict.VCPU_CORE).getUsed().set(coreUsed);
    }

    // todo
    public void countGpuResource() {

    }


    public List<Integer> gpuProcessorUsed() {
        return sysInfo.countGpuProcessors();
    }

    public ErServerNode queryNodeResource(ErServerNode erServerNode) {
        ErServerNode newErServerNode = new ErServerNode();
        try {
            BeanUtils.copyProperties(newErServerNode, erServerNode);
        } catch (InvocationTargetException | IllegalAccessException e) {
            logger.error("copyProperties error: {}", e.getMessage());
        }
        newErServerNode.setId(NodeManagerMeta.serverNodeId);
        Iterator<ResourceWrapper> iterator = resourceMap.values().iterator();
        List<ErResource> resources = new ArrayList<>();
        while (iterator.hasNext()) {
            ResourceWrapper wrapper = iterator.next();
            if (wrapper.getTotal().get() != -1) {
                ErResource erResource = new ErResource();
                erResource.setResourceType(wrapper.getResourceType());
                erResource.setTotal(wrapper.getTotal().get());
                erResource.setUsed(wrapper.getUsed().get());
                erResource.setAllocated(wrapper.getAllocated().get());
                resources.add(erResource);
            }
        }
        newErServerNode.setResources(resources);
        return newErServerNode;
    }

    @Override
    public void run(String[] args) {
        NodeManagerMeta.loadNodeManagerMetaFromFile();
        NodeManagerTask.runTask(heartBeatThread);
        NodeManagerTask.runTask(resourceCountThread);
    }

    class ResourceCountThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    countCpuResource();
                    countMemoryResource();
                    countGpuResource();
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("register node error");
                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @PreDestroy
    public ErNodeHeartbeat tryNodeHeartbeat() {
        String nodeHost = MetaInfo.CONFKEY_NODE_MANAGER_HOST == null ? NetUtils.getLocalHost(MetaInfo.CONFKEY_NODE_MANAGER_NET_DEVICE) : MetaInfo.CONFKEY_NODE_MANAGER_HOST;
        int nodePort = MetaInfo.CONFKEY_NODE_MANAGER_PORT;
        ErEndpoint endpoint = new ErEndpoint(nodeHost, nodePort);
        ErServerNode erServerNode = new ErServerNode(NodeManagerMeta.serverNodeId, Dict.NODE_MANAGER, endpoint, NodeManagerMeta.status);
        ErNodeHeartbeat nodeHeartbeat = new ErNodeHeartbeat(seq, queryNodeResource(erServerNode));
        nodeHeartbeat.setGpuProcessors(gpuProcessorUsed());
        return client.nodeHeartbeat(new Context(), nodeHeartbeat);
    }

    class HeartBeatThread extends Thread {
        Thread currentGrpcThread = null;
        @Override
        public void run() {
            while (true) {
                try {
                    seq += 1;
                    if (currentGrpcThread != null && currentGrpcThread.isAlive()) {
                        currentGrpcThread.interrupt();
                    }
                    currentGrpcThread = new Thread(() -> {
                        ErNodeHeartbeat nodeHeartBeat = tryNodeHeartbeat();
                        if (nodeHeartBeat != null && nodeHeartBeat.getNode() != null) {
                            if (NodeManagerMeta.status.equals(Dict.INIT)) {
                                if (nodeHeartBeat.getNode().getId() != -1) {
                                    NodeManagerMeta.serverNodeId = nodeHeartBeat.getNode().getId();
                                    NodeManagerMeta.clusterId = nodeHeartBeat.getNode().getClusterId();
                                    NodeManagerMeta.refreshServerNodeMetaIntoFile();
                                    NodeManagerMeta.status = Dict.HEALTHY;
                                    logger.info("get node id {} from cluster-manager", NodeManagerMeta.serverNodeId);
                                }
                            }
                        }
                    });
                    currentGrpcThread.start();
                } catch (Exception e) {
                    logger.error("node heart beat error {}", e.getMessage());
                }
                try {
                    Thread.sleep(MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    logger.error("node heart beat error {}", e.getMessage());
                }
            }
        }
    }

}
