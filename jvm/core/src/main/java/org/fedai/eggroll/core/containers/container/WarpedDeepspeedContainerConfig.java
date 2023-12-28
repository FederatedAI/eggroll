package org.fedai.eggroll.core.containers.container;

import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.pojo.DeepspeedContainerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WarpedDeepspeedContainerConfig {
    private List<Integer> cudaVisibleDevices;
    private int worldSize;
    private int crossRank;
    private int crossSize;
    private int localSize;
    private int localRank;
    private int rank;
    private StoreConfig storeConfig;
    private String backend;
    Logger logger = LoggerFactory.getLogger(WarpedDeepspeedContainerConfig.class);

    private Map<String, String> containerResourceEnvironments = new HashMap<>();

    public WarpedDeepspeedContainerConfig(List<Integer> cudaVisibleDevices, int worldSize, int crossRank, int crossSize,
                                          int localSize, int localRank, int rank, StoreConfig storeConfig,
                                          String backend) {
        this.cudaVisibleDevices = cudaVisibleDevices;
        this.worldSize = worldSize;
        this.crossRank = crossRank;
        this.crossSize = crossSize;
        this.localSize = localSize;
        this.localRank = localRank;
        this.rank = rank;
        this.storeConfig = storeConfig;
        this.backend = backend;
    }

    public WarpedDeepspeedContainerConfig(DeepspeedContainerConfig deepspeedContainerConfig) {

        this.cudaVisibleDevices = deepspeedContainerConfig.getCudaVisibleDevices();
        this.worldSize = deepspeedContainerConfig.getWorldSize();
        this.crossRank = deepspeedContainerConfig.getCrossRank();
        this.crossSize = deepspeedContainerConfig.getCrossSize();
        this.localSize = deepspeedContainerConfig.getLocalSize();
        this.localRank = deepspeedContainerConfig.getLocalRank();
        this.rank = deepspeedContainerConfig.getRank();
        this.storeConfig = new StoreConfig(deepspeedContainerConfig.getStoreHost(),
                deepspeedContainerConfig.getStorePort(),
                deepspeedContainerConfig.getStorePrefix());
        this.backend = deepspeedContainerConfig.getBackend();
    }

    private String getBackend() {
        return StringUtils.isNotBlank(backend) ? backend : MetaInfo.EGGROLL_CONTAINER_DEEPSPEED_TORCH_DISTRIBUTED_BACKEND;
    }

    public Map<String, String> getPytorchDistributedEnvironments() {
        Map<String, String> envMap = new HashMap<>();
        envMap.put("WORLD_SIZE", Integer.toString(worldSize));
        envMap.put("CROSS_RANK", Integer.toString(crossRank));
        envMap.put("CROSS_SIZE", Integer.toString(crossSize));
        envMap.put("LOCAL_SIZE", Integer.toString(localSize));
        envMap.put("LOCAL_RANK", Integer.toString(localRank));
        envMap.put("RANK", Integer.toString(rank));
        envMap.put("CUDA_VISIBLE_DEVICES", String.join(",", cudaVisibleDevices.stream().map(Object::toString).collect(Collectors.toList())));
        return envMap;
    }

    public Map<String, String> getEggrollCustomizedEnvironments() {
        Map<String, String> envMap = new HashMap<>();
        envMap.put("EGGROLL_DEEPSPEED_STORE_HOST", StringUtils.isNotBlank(storeConfig.getHost()) ? storeConfig.getHost() : MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST);
        envMap.put("EGGROLL_DEEPSPEED_STORE_PORT", Integer.toString(storeConfig.getPort() != null && storeConfig.getPort() != 0 ? storeConfig.getPort() : MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));
        envMap.put("EGGROLL_DEEPSPEED_STORE_PREFIX", storeConfig.getPrefix());
        envMap.put("EGGROLL_DEEPSPEED_BACKEND", getBackend());
        return envMap;
    }

    public Map<String, String> getEggrollContainerResourceEnvironments() {
        return containerResourceEnvironments;
    }

    public void setEggrollContainerResourceLogsDir(String logsDir) {
        containerResourceEnvironments.put("EGGROLL_DEEPSPEED_CONTAINER_LOGS_DIR", logsDir);
    }

    public void setEggrollContainerResourceModelsDir(String modelsDir) {
        containerResourceEnvironments.put("EGGROLL_DEEPSPEED_CONTAINER_MODELS_DIR", modelsDir);
    }

    public void setEggrollContainerResourceResultDir(String resultsDir) {
        containerResourceEnvironments.put("EGGROLL_DEEPSPEED_CONTAINER_RESULT_DIR", resultsDir);
    }
}
