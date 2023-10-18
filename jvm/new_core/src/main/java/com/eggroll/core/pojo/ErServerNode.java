package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class ErServerNode implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ErServerNode.class);
    private Long id;
    private String name;
    private Long clusterId;
    private ErEndpoint endpoint;
    private String nodeType;
    private String status;
    private Date lastHeartBeat;
    private List<ErResource> resources;

    public ErServerNode() {
        this.id = -1L;
        this.name = StringConstants.EMPTY;
        this.clusterId = 0L;
        this.endpoint = new ErEndpoint(StringConstants.EMPTY, -1);
        this.nodeType = StringConstants.EMPTY;
        this.status = StringConstants.EMPTY;
        this.lastHeartBeat = null;
        this.resources = new ArrayList<>();
    }

    public ErServerNode(Long id, String name, Long clusterId, ErEndpoint endpoint,
                        String nodeType, String status, Date lastHeartBeat,
                        List<ErResource> resources) {
        this.id = id;
        this.name = name;
        this.clusterId = clusterId;
        this.endpoint = endpoint;
        this.nodeType = nodeType;
        this.status = status;
        this.lastHeartBeat = lastHeartBeat;
        this.resources = resources;
    }

    public ErServerNode(String nodeType, String status) {
        this.id = -1L;
        this.name = StringConstants.EMPTY;
        this.clusterId = 0L;
        this.endpoint = new ErEndpoint(StringConstants.EMPTY, -1);
        this.nodeType = nodeType;
        this.status = status;
        this.lastHeartBeat = null;
        this.resources = new ArrayList<>();
    }

    public ErServerNode(Long id, String nodeType, ErEndpoint endpoint, String status) {
        this.id = id;
        this.endpoint = endpoint;
        this.nodeType = nodeType;
        this.status = status;
    }

    @Override
    public String toString() {
        return "<ErServerNode(id=" + id + ", name=" + name +
                ", clusterId=" + clusterId + ", endpoint=" + endpoint +
                ", nodeType=" + nodeType + ", status=" + status +
                ", lastHeartBeat=" + lastHeartBeat +
                ", resources=" + JsonUtil.object2Json(resources) +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    public Meta.ServerNode toProto() {
        Meta.ServerNode.Builder builder = Meta.ServerNode.newBuilder();
        if (this.id != null)
            builder.setId(this.id);
        if (this.name != null)
            builder.setName(this.name);
        if (this.clusterId != null)
            builder.setClusterId(this.clusterId);
        if (this.nodeType != null)
            builder.setNodeType(this.nodeType);
        if (this.status != null)
            builder.setStatus(this.status);
        if (this.endpoint != null)
            builder.setEndpoint(endpoint.toProto());
        if(this.resources != null){
            builder.addAllResources(this.resources.stream().map(ErResource::toProto).collect(Collectors.toList()));
        }
        return builder.build();
    }

    public static ErServerNode fromProto(Meta.ServerNode serverNode) {
        ErServerNode erServerNode = new ErServerNode();
        erServerNode.deserialize(serverNode.toByteArray());
        return erServerNode;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.ServerNode serverNode = Meta.ServerNode.parseFrom(data);
            this.id = serverNode.getId();
            this.clusterId = serverNode.getClusterId();
            this.name = serverNode.getName();
            this.nodeType = serverNode.getNodeType();
            this.status = serverNode.getStatus();
            this.endpoint = ErEndpoint.fromProto(serverNode.getEndpoint());
            this.resources = serverNode.getResourcesList().stream().map(ErResource::fromProto).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}