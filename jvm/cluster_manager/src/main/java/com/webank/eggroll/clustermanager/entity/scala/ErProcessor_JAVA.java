package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;

import java.util.*;

@Data
public class ErProcessor_JAVA implements NetworkingRpcMessage_JAVA {
    private long id = -1;
    private String sessionId = StringConstants.EMPTY();
    private long serverNodeId = -1;
    private String name = StringConstants.EMPTY();
    private String processorType = StringConstants.EMPTY();
    private String status = StringConstants.EMPTY();
    private ErEndpoint_JAVA commandEndpoint = null;
    private ErEndpoint_JAVA transferEndpoint = null;
    private int pid = -1;
    private Map<String, String> options = new HashMap<>();
    private String tag = StringConstants.EMPTY();
    private List<ErResource_JAVA> resources = new ArrayList<>();
    private Date createdAt = null;
    private Date updatedAt = null;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (resources != null) {
            for (ErResource_JAVA resource : resources) {
                sb.append(resource.toString());
            }
        }
        //TODO List的输出
        return "<ErProcessor(id=" + id + ", sessionId=" + sessionId +
                ", serverNodeId=" + serverNodeId + ", name=" + name +
                ", processorType=" + processorType + ", status=" + status +
                ", commandEndpoint=" + commandEndpoint + ", transferEndpoint=" + transferEndpoint +
                ", createdAt=" + createdAt + ", updatedAt=" + updatedAt +
                ", pid=" + pid + ", options=" + options + ", tag=" + tag +
                ") at " + Integer.toHexString(hashCode()) + " resources " + sb.toString() + ">";
    }

    public Meta.Processor toProto() {
        Meta.Processor.Builder builder = Meta.Processor.newBuilder()
                .setId(this.getId())
                .setServerNodeId(this.getServerNodeId())
                .setName(this.getName())
                .setProcessorType(this.getProcessorType())
                .setStatus(this.getStatus())
                .setCommandEndpoint(this.getCommandEndpoint() != null ? this.getCommandEndpoint().toProto() : Meta.Endpoint.getDefaultInstance())
                .setTransferEndpoint(this.getTransferEndpoint() != null ? this.getTransferEndpoint().toProto() : Meta.Endpoint.getDefaultInstance())
                .setPid(this.getPid())
                .putAllOptions(this.getOptions())
                .setTag(this.getTag());

        return builder.build();
    }
}