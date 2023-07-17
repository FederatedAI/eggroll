package com.webank.eggroll.clustermanager.dao.impl.dao;

import com.google.common.collect.Lists;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.core.meta.ErEndpoint;
import com.webank.eggroll.core.meta.ErResource;
import com.webank.eggroll.core.meta.ErServerCluster;
import com.webank.eggroll.core.meta.ErServerNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ResourceDaoNew {

    @Autowired
    ServerNodeService serverNodeService;

    public synchronized ErServerCluster getServerCluster() {
        return getServerCluster(0L);
    }

    public synchronized ErServerCluster getServerCluster(Long clusterId) {
        List<ServerNode> nodes = serverNodeService.listByIds(Lists.newArrayList(clusterId));
        ErServerNode[] erServerNodes = new ErServerNode[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            ServerNode node = nodes.get(i);
            ErServerNode erServerNode = new ErServerNode(node.getServerNodeId(), node.getName(), clusterId
                    , new ErEndpoint(node.getHost(), node.getPort()), node.getNodeType(), node.getStatus(), null, new ErResource[0]);
            erServerNodes[i] = erServerNode;
        }
        return new ErServerCluster(clusterId, erServerNodes);
    }

}
