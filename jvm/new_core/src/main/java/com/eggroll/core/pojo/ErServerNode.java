package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import com.eggroll.core.utils.JsonUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
public class ErServerNode implements RpcMessage {
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

    @Override
    public String toString() {
        return "<ErServerNode(id=" + id + ", name=" + name +
                ", clusterId=" + clusterId + ", endpoint=" + endpoint +
                ", nodeType=" + nodeType + ", status=" + status +
                ", lastHeartBeat=" + lastHeartBeat +
                ", resources=" + JsonUtil.object2Json(resources) +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    @Override
    public byte[] serialize() {
        Meta.ServerNode.Builder builder = Meta.ServerNode.newBuilder();
        builder.setId(this.id).setName(this.name).setClusterId(this.clusterId)
                .setNodeType(this.nodeType).setStatus(this.status);
        if (this.endpoint != null) {
            builder.setEndpoint(endpoint.toProto());
        }
        return builder.build().toByteArray();
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
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}