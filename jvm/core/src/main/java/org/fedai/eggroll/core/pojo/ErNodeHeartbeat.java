package org.fedai.eggroll.core.pojo;


import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class ErNodeHeartbeat implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ErNodeHeartbeat.class);
    private long id;
    private ErServerNode node;
    private List<Integer> gpuProcessors;
    private List<Integer> cpuProcessors;

    public ErNodeHeartbeat() {
        this.id = -1;
        this.node = null;
    }

    public ErNodeHeartbeat(long id, ErServerNode node) {
        this.id = id;
        this.node = node;
    }


    @Override
    public String toString() {
        return "<ErNodeHeartbeat(id=" + id + ", node=" + node +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    public Meta.NodeHeartbeat toProto() {
        Meta.NodeHeartbeat.Builder builder = Meta.NodeHeartbeat.newBuilder();
        builder.setId(this.id).setNode(this.node.toProto());
        if (cpuProcessors != null) {
            builder.addAllCpuProcessors(cpuProcessors);
        }
        if (gpuProcessors != null) {
            builder.addAllGpuProcessors(gpuProcessors);
        }
        return builder.build();
    }

    public static ErNodeHeartbeat fromProto(Meta.NodeHeartbeat nodeHeartbeat) {
        ErNodeHeartbeat erNodeHeartbeat = new ErNodeHeartbeat();
        erNodeHeartbeat.deserialize(nodeHeartbeat.toByteArray());
        return erNodeHeartbeat;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.NodeHeartbeat nodeHeartbeat = Meta.NodeHeartbeat.parseFrom(data);
            this.id = nodeHeartbeat.getId();
            ErServerNode erServerNode = new ErServerNode();
            erServerNode.deserialize(nodeHeartbeat.getNode().toByteArray());
            this.node = erServerNode;
            this.gpuProcessors = nodeHeartbeat.getGpuProcessorsList();
            this.cpuProcessors = nodeHeartbeat.getCpuProcessorsList();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}