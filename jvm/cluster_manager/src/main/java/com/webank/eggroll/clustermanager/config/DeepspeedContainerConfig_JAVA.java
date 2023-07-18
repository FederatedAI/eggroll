package com.webank.eggroll.clustermanager.config;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;

import java.util.List;

@Data
public class DeepspeedContainerConfig_JAVA {
    private List<Integer> cudaVisibleDevices;
    private int worldSize;
    private int crossRank;
    private int crossSize;
    private int localSize;
    private int localRank;
    private int rank;
    private String storePrefix;
    private String storeHost;
    private Integer storePort;
    private String backend;

    public DeepspeedContainerConfig_JAVA(List<Integer> cudaVisibleDevices, int worldSize, int crossRank, int crossSize,
                                    int localSize, int localRank, int rank, String storePrefix) {
        this.cudaVisibleDevices = cudaVisibleDevices;
        this.worldSize = worldSize;
        this.crossRank = crossRank;
        this.crossSize = crossSize;
        this.localSize = localSize;
        this.localRank = localRank;
        this.rank = rank;
        this.storePrefix = storePrefix;
    }

    public DeepspeedContainerConfig_JAVA(List<Integer> cudaVisibleDevices, int worldSize, int crossRank, int crossSize,
                                    int localSize, int localRank, int rank, String storePrefix,
                                    String storeHost, int storePort, String backend) {
        this(cudaVisibleDevices, worldSize, crossRank, crossSize, localSize, localRank, rank, storePrefix);
        this.storeHost = storeHost;
        this.storePort = storePort;
        this.backend = backend;
    }

    public static byte[] serialize(DeepspeedContainerConfig_JAVA src) {
        Containers.DeepspeedContainerConfig.Builder builder = Containers.DeepspeedContainerConfig.newBuilder();
        builder.addAllCudaVisibleDevices(src.getCudaVisibleDevices())
                .setWorldSize(src.getWorldSize())
                .setCrossRank(src.getCrossRank())
                .setCrossSize(src.getCrossSize())
                .setLocalSize(src.getLocalSize())
                .setLocalRank(src.getLocalRank())
                .setRank(src.getRank())
                .setStoreHost(src.getStoreHost() != null ? src.getStoreHost() : "")
                .setStorePort(src.getStorePort() != null ? src.getStorePort() : -1)
                .setBackend(src.getBackend() != null ? src.getBackend() : "")
                .setStorePrefix(src.getStorePrefix());

        Containers.DeepspeedContainerConfig deepspeedContainerConfig = builder.build();
        return deepspeedContainerConfig.toByteArray();
    }

    public static DeepspeedContainerConfig_JAVA deserialize(ByteString byteString) {
        try {
            Containers.DeepspeedContainerConfig src = Containers.DeepspeedContainerConfig.parseFrom(byteString);
            return new DeepspeedContainerConfig_JAVA(
                    src.getCudaVisibleDevicesList(),
                    src.getWorldSize(),
                    src.getCrossRank(),
                    src.getCrossSize(),
                    src.getLocalSize(),
                    src.getLocalRank(),
                    src.getRank(),
                    src.getStorePrefix(),
                    src.getStoreHost(),
                    src.getStorePort(),
                    src.getBackend()
            );
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
        }
    }
}
