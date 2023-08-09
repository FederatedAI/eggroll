package com.webank.eggroll.nodemanager.service;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.grpc.ClusterManagerClient;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErNodeHeartbeat;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.utils.NetUtils;
import com.webank.eggroll.nodemanager.env.Shell;
import com.webank.eggroll.nodemanager.env.SysInfoLinux;
import com.webank.eggroll.nodemanager.meta.NodeManagerMeta;
import com.webank.eggroll.nodemanager.pojo.ResourceWrapper;
import com.webank.eggroll.nodemanager.processor.DefaultProcessorManager;
import com.webank.eggroll.nodemanager.utils.GetSystemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class NodeResourceManager {

    Logger logger = LoggerFactory.getLogger(NodeResourceManager.class);
    @Autowired
    private SysInfoLinux sysInfo;

    ClusterManagerClient client;

    //todo
    Long physicalMemorySize = 0L;

    Map<String, ResourceWrapper> resourceMap = new HashMap<>();

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
        return resourceWrapper.getAllocated().getAndAdd(-count);
    }

    // todo
    public void start() {

    }

    public Long getPhysicalMemorySize() {
        if (Shell.LINUX) {
            return sysInfo.getPhysicalMemorySize();
        } else {
            return GetSystemInfo.getTotalMemorySize();
        }
    }

    public Long getGpuSize() {
        Long defaultSize = 0L;
        if (Shell.LINUX) {
            try {
                defaultSize = Long.valueOf(sysInfo.getGpuNumber());
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

    // todo  physicalMemorySize--->init
    public void countMemoryResource() {
        Long available = 0L;
        if (Shell.LINUX) {
            available = sysInfo.getAvailablePhysicalMemorySize();
        } else {
            resourceMap.get(Dict.PHYSICAL_MEMORY).getUsed().set(physicalMemorySize - available);
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

    //todo
    public ErServerNode queryNodeResource(ErServerNode erServerNode) {
        ErServerNode newErServerNode = new ErServerNode();
        BeanUtils.copyProperties(erServerNode, newErServerNode);
        newErServerNode.setId(NodeManagerMeta.serverNodeId);

        return newErServerNode;
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

    //todo
    class HeartBeatThread extends Thread {


        @Override
        public void run() {
            Boolean notOver = true;
            Long seq = 0L;
            while (notOver) {

            }
        }
    }

}

