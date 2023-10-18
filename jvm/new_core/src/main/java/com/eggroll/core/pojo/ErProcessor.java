package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Data
public class ErProcessor implements RpcMessage {

    private Long id = -1L;
    private String sessionId = StringConstants.EMPTY;
    private Long serverNodeId = -1L;
    private String name = StringConstants.EMPTY;
    private String processorType = StringConstants.EMPTY;
    private String status = StringConstants.EMPTY;
    private ErEndpoint commandEndpoint = null;
    private ErEndpoint transferEndpoint = null;
    private Integer pid = -1;
    private Map<String, String> options = new HashMap<>();
    private String tag = StringConstants.EMPTY;
    private List<ErResource> resources = new ArrayList<>();
    private Date createdAt = null;
    private Date updatedAt = null;


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (resources != null) {
            for (ErResource resource : resources) {
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

    public static ErProcessor fromProto(Meta.Processor processor) {
        ErProcessor erProcessor = new ErProcessor();
        erProcessor.deserialize(processor.toByteArray());
        return erProcessor;
    }

    @Override
    public byte[] serialize() {
        return this.toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.Processor processor = Meta.Processor.parseFrom(data);
            this.id = processor.getId();
            this.serverNodeId = processor.getServerNodeId();
            this.name = processor.getName();
            this.processorType = processor.getProcessorType();
            this.status = processor.getStatus();
            if (processor.getCommandEndpoint() != null) {
                this.commandEndpoint = ErEndpoint.fromProto(processor.getCommandEndpoint());
            }
            if (processor.getTransferEndpoint() != null) {
                this.transferEndpoint = ErEndpoint.fromProto(processor.getTransferEndpoint());
            }
            this.options = processor.getOptionsMap();
            this.pid = processor.getPid();
            this.tag= processor.getTag();
        } catch (InvalidProtocolBufferException e) {

        }
    }

}