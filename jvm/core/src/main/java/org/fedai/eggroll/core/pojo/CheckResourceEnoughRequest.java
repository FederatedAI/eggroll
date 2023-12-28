package org.fedai.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.fedai.eggroll.core.config.Dict;

@Data
public class CheckResourceEnoughRequest implements RpcMessage {

    /**
     * cpu/gpu/memory
     */
    private String resourceType = Dict.RESOURCE_TYPE_GPU;

    private Long requiredResourceCount = 0L;

    /**
     * clusterCheck:整个集群总的资源是否满足
     * nodeCheck:是否有单个节点满足
     */
    private String checkType = Dict.CHECK_RESOURCE_ENOUGH_CHECK_TYPE_NODE;

    @Override
    public byte[] serialize() {
        Meta.CheckResourceEnoughRequest.Builder builder = Meta.CheckResourceEnoughRequest.newBuilder();
        builder.setResourceType(resourceType);
        builder.setRequiredResourceCount(requiredResourceCount);
        builder.setCheckType(checkType);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.CheckResourceEnoughRequest request = Meta.CheckResourceEnoughRequest.parseFrom(data);
            this.resourceType = request.getResourceType();
            this.requiredResourceCount = request.getRequiredResourceCount();
            this.checkType = request.getCheckType();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "CheckResourceEnoughRequest{" +
                "resourceType='" + resourceType + '\'' +
                ", requiredResourceCount=" + requiredResourceCount +
                ", checkType='" + checkType + '\'' +
                '}';
    }
}
