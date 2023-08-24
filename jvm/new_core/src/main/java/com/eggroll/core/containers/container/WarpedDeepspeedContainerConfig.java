package com.eggroll.core.containers.container;

import com.eggroll.core.pojo.StaticErConf;

import java.util.*;
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
        return Optional.ofNullable(backend).orElse(
                StaticErConf.getString(Container.ContainerKey.DEEPSPEED_TORCH_DISTRIBUTED_BACKEND, "nccl"));
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
        envMap.put("EGGROLL_DEEPSPEED_STORE_HOST", storeConfig.getHost());
        envMap.put("EGGROLL_DEEPSPEED_STORE_PORT", Integer.toString(storeConfig.getPort()));
        envMap.put("EGGROLL_DEEPSPEED_STORE_PREFIX", storeConfig.getPrefix());
        envMap.put("EGGROLL_DEEPSPEED_BACKEND", getBackend());
        return envMap;
    }
}
