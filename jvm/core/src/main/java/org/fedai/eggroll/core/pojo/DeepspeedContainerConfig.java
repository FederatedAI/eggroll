package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class DeepspeedContainerConfig implements RpcMessage {
    Logger log = LoggerFactory.getLogger(DeepspeedContainerConfig.class);
    private List<Integer> cudaVisibleDevices;
    private Integer worldSize;
    private Integer crossRank;
    private Integer crossSize;
    private Integer localSize;
    private Integer localRank;
    private Integer rank;
    private String storePrefix;
    private String storeHost;
    private Integer storePort;
    private String backend;

    public Containers.DeepspeedContainerConfig toProto() {

        Containers.DeepspeedContainerConfig.Builder builder = Containers.DeepspeedContainerConfig.newBuilder();
        if (CollectionUtils.isNotEmpty(this.cudaVisibleDevices)) {
            builder.addAllCudaVisibleDevices(this.cudaVisibleDevices);
        }
        if (this.worldSize != null) {
            builder.setWorldSize(this.worldSize);
        }
        if (this.crossRank != null) {
            builder.setCrossRank(this.crossRank);
        }
        if (this.crossSize != null) {
            builder.setCrossSize(this.crossSize);
        }
        if (this.localSize != null) {
            builder.setLocalSize(this.localSize);
        }
        if (this.localRank != null) {
            builder.setLocalRank(this.localRank);
        }
        if (this.rank != null) {
            builder.setRank(this.rank);
        }
        if (StringUtils.isNotBlank(this.storeHost)) {
            builder.setStoreHost(this.storeHost);
        }
        if (this.storePort != null) {
            builder.setStorePort(this.storePort);
        }
        if (StringUtils.isNotBlank(this.backend)) {
            builder.setBackend(this.backend);
        }
        if (StringUtils.isNotBlank(this.storePrefix)) {
            builder.setStorePrefix(this.storePrefix);
        }
        return builder.build();
    }

    public static DeepspeedContainerConfig fromProto(Containers.DeepspeedContainerConfig deepspeedContainerConfig) {
        DeepspeedContainerConfig result = new DeepspeedContainerConfig();
        result.deserialize(deepspeedContainerConfig.toByteArray());
        return result;
    }


    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.DeepspeedContainerConfig deepspeedContainerConfig = Containers.DeepspeedContainerConfig.parseFrom(data);
            this.cudaVisibleDevices = deepspeedContainerConfig.getCudaVisibleDevicesList();
            this.worldSize = deepspeedContainerConfig.getWorldSize();
            this.crossRank = deepspeedContainerConfig.getCrossRank();
            this.crossSize = deepspeedContainerConfig.getCrossSize();
            this.localSize = deepspeedContainerConfig.getLocalSize();
            this.localRank = deepspeedContainerConfig.getLocalRank();
            this.rank = deepspeedContainerConfig.getRank();
            this.storePrefix = deepspeedContainerConfig.getStorePrefix();
            this.storeHost = deepspeedContainerConfig.getStoreHost();
            this.storePort = deepspeedContainerConfig.getStorePort();
            this.backend = deepspeedContainerConfig.getBackend();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }

}
