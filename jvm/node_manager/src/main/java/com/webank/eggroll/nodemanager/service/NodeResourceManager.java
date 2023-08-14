package com.webank.eggroll.nodemanager.service;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.grpc.ClusterManagerClient;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErNodeHeartbeat;
import com.eggroll.core.pojo.ErResource;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.utils.NetUtils;
import com.webank.eggroll.nodemanager.env.Shell;
import com.webank.eggroll.nodemanager.env.SysInfoLinux;
import com.webank.eggroll.nodemanager.meta.NodeManagerMeta;
import com.webank.eggroll.nodemanager.pojo.ResourceWrapper;
import com.webank.eggroll.nodemanager.utils.GetSystemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class NodeResourceManager implements ApplicationListener<ApplicationReadyEvent> {

    Logger logger = LoggerFactory.getLogger(NodeResourceManager.class);
    @Autowired
    private SysInfoLinux sysInfo;

    ClusterManagerClient client;

    Long physicalMemorySize;
    HeartBeatThread heartBeatThread;
    ResourceCountThread resourceCountThread;

    Map<String, ResourceWrapper> resourceMap;

    public NodeResourceManager() {

        int cpus = MetaInfo.CONFKEY_NODE_MANAGER_CPU_VCORES == null ? getAvailableProcessors() : MetaInfo.CONFKEY_NODE_MANAGER_CPU_VCORES;
        int gpus = MetaInfo.CONFKEY_NODE_MANAGER_GPU_VCORES == null ? getGpuSize() : MetaInfo.CONFKEY_NODE_MANAGER_GPU_VCORES;
        ResourceWrapper cpuCore = new ResourceWrapper(Dict.VCPU_CORE, new AtomicLong(cpus));
        ResourceWrapper physicalMemory = new ResourceWrapper(Dict.PHYSICAL_MEMORY, new AtomicLong(getPhysicalMemorySize()));
        ResourceWrapper gpuCore = new ResourceWrapper(Dict.VGPU_CORE, new AtomicLong(gpus));
        resourceMap = new HashMap<>();
        resourceMap.put(Dict.VCPU_CORE, cpuCore);
        resourceMap.put(Dict.PHYSICAL_MEMORY, physicalMemory);
        resourceMap.put(Dict.VGPU_CORE, gpuCore);
        client = new ClusterManagerClient (new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST,MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));
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
            if (required <= left) {
                return true;
            }
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

    public void start() {
        NodeManagerMeta.loadNodeManagerMetaFromFile();
        heartBeatThread.start();
        resourceCountThread.start();
    }

    public Long getPhysicalMemorySize() {
        if (Shell.LINUX) {
            return sysInfo.getPhysicalMemorySize();
        } else {
            return GetSystemInfo.getTotalMemorySize();
        }
    }

    public Integer getGpuSize() {
        Integer defaultSize = 0;
        if (Shell.LINUX) {
            try {
                defaultSize = sysInfo.getGpuNumber();
            } catch (IOException e) {
                logger.error("get gpuSize failed: {}", e.getMessage());
            }
        }
        return defaultSize;
    }

    public Long getAvailablePhysicalMemorySize() {
        if (Shell.LINUX) {
            return sysInfo.getAvailablePhysicalMemorySize();
        } else {
            return GetSystemInfo.getFreePhysicalMemorySize();
        }
    }

    public int getAvailableProcessors() {
        if (Shell.LINUX) {
            return sysInfo.getNumCores();
        } else {
            return GetSystemInfo.getAvailableProcessors();
        }
    }

    public void countMemoryResource() {
        Long available = 0L;
        if (Shell.LINUX) {
            available = sysInfo.getAvailablePhysicalMemorySize();
        } else {
            available = GetSystemInfo.getFreePhysicalMemorySize();
        }
        resourceMap.get(Dict.PHYSICAL_MEMORY).getUsed().set(physicalMemorySize - available);
    }

    public void countCpuResource() {
        int coreUsed = 0;
        if (Shell.LINUX) {
            coreUsed = (int) sysInfo.getNumVCoresUsed();
        } else {
            coreUsed = (int) GetSystemInfo.getProcessCpuLoad();
        }
        resourceMap.get(Dict.VCPU_CORE).getUsed().set(coreUsed);
    }

    // todo
    public void countGpuResource() {

    }

    public ErServerNode queryNodeResource(ErServerNode erServerNode) {
        ErServerNode newErServerNode = new ErServerNode();
        BeanUtils.copyProperties(erServerNode, newErServerNode);
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
    public void onApplicationEvent(ApplicationReadyEvent event) {
        this.start();
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

    class HeartBeatThread extends Thread {
        public ErNodeHeartbeat generateNodeBeat(Long seq) {
            String nodeHost = MetaInfo.CONFKEY_NODE_MANAGER_HOST == null ? NetUtils.getLocalHost() : MetaInfo.CONFKEY_NODE_MANAGER_HOST;
            int nodePort = MetaInfo.CONFKEY_NODE_MANAGER_PORT;
            ErEndpoint endpoint = new ErEndpoint(nodeHost, nodePort);
            ErServerNode erServerNode = new ErServerNode(NodeManagerMeta.serverNodeId, Dict.NODE_MANAGER, endpoint, NodeManagerMeta.status);
            ErNodeHeartbeat nodeHeartbeat = new ErNodeHeartbeat(seq, queryNodeResource(erServerNode));
            return nodeHeartbeat;
        }

        @Override
        public void run() {
            Boolean notOver = true;
            Long seq = 0L;
            while (notOver) {
                try {
                    seq += 1;
                    ErNodeHeartbeat nodeHeartBeat = client.nodeHeartbeat(generateNodeBeat(seq));
                    logger.info("send node heart beat to cluster-manager");
                    if (nodeHeartBeat != null && nodeHeartBeat.getNode() != null) {
                        if (NodeManagerMeta.status.equals(Dict.INIT)) {
                            if (nodeHeartBeat.getNode().getId() != -1) {
                                NodeManagerMeta.serverNodeId = nodeHeartBeat.getNode().getId();
                                NodeManagerMeta.clusterId = nodeHeartBeat.getNode().getClusterId();
                                NodeManagerMeta.refreshServerNodeMetaIntoFile();
                                NodeManagerMeta.status = Dict.HEALTHY;
                            }
                            logger.info("get node id {} from cluster-manager", NodeManagerMeta.serverNodeId);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

