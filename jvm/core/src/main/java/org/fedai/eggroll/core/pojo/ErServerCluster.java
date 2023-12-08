package org.fedai.eggroll.core.pojo;


import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.fedai.eggroll.core.config.Dict;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class ErServerCluster implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ErServerCluster.class);
    private long id;
    private String name;
    private List<ErServerNode> serverNodes;
    private String tag;


    public ErServerCluster() {
        this.id = -1;
        this.name = Dict.EMPTY;
        this.serverNodes = new ArrayList<>();
        this.tag = Dict.EMPTY;
    }

    public ErServerCluster(long id, List<ErServerNode> serverNodes, String tag) {
        this.id = id;
        this.name = Dict.EMPTY;
        this.serverNodes = serverNodes;
        this.tag = tag;
    }

    @Override
    public String toString() {//TODO List的输出
        return "<ErServerCluster(id=" + id + ", name=" + name +
                ", serverNodes=" + serverNodes.toString() + ", tag=" + tag +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    public Meta.ServerCluster toProto() {
        Meta.ServerCluster.Builder builder = Meta.ServerCluster.newBuilder();
        builder.setId(this.id)
                .setName(this.name)
                .addAllServerNodes(this.serverNodes.stream().map(ErServerNode::toProto).collect(Collectors.toList()))
                .setTag(this.tag);
        return builder.build();
    }

    public static ErServerCluster fromProto(Meta.ServerCluster serverCluster) {
        ErServerCluster erServerCluster = new ErServerCluster();
        erServerCluster.deserialize(serverCluster.toByteArray());
        return erServerCluster;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.ServerCluster serverCluster = Meta.ServerCluster.parseFrom(data);
            this.id = serverCluster.getId();
            this.name = serverCluster.getName();
            this.serverNodes = serverCluster.getServerNodesList().stream().map(ErServerNode::fromProto).collect(Collectors.toList());
            this.tag = serverCluster.getTag();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}