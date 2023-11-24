package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Data
public class ServerCluster implements RpcMessage {

    private List<ErServerNode> serverNodes;

    public ServerCluster() {

    }

    public ServerCluster(List<ErServerNode> serverNodes) {
        this.serverNodes = serverNodes;
    }

    public Meta.ServerCluster toProto() {
        Meta.ServerCluster.Builder builder = Meta.ServerCluster.newBuilder();
        List<Meta.ServerNode> list = new ArrayList<>();
        serverNodes.forEach(erServerNode -> {
            list.add(erServerNode.toProto());
        });
        builder.addAllServerNodes(list);
        return builder.build();
    }

    public ServerCluster fromProto(Meta.ServerCluster serverCluster) {
        ServerCluster cluster = new ServerCluster();
        cluster.deserialize(serverCluster.toByteArray());
        return cluster;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }


    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.ServerCluster response = Meta.ServerCluster.parseFrom(data);
            List<Meta.ServerNode> serverNodesList = response.getServerNodesList();
            if (CollectionUtils.isNotEmpty(serverNodesList)) {
                List<ErServerNode> serverNodeList = new ArrayList<>();
                serverNodesList.forEach(serverNode -> {
                    serverNodeList.add(ErServerNode.fromProto(serverNode));
                });
                this.serverNodes = serverNodeList;
            }
        } catch (Exception e) {

        }

    }
}
