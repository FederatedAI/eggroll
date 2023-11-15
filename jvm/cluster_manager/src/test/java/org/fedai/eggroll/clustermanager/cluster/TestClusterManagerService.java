package org.fedai.eggroll.clustermanager.cluster;

import org.fedai.eggroll.core.constant.ServerNodeStatus;
import org.fedai.eggroll.core.constant.ServerNodeTypes;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErNodeHeartbeat;
import org.fedai.eggroll.core.pojo.ErServerNode;
import org.junit.Test;


public class TestClusterManagerService {


    ClusterManagerService clusterManagerService;

    @Test
    public void testNodeHeartbeat() {
        ErNodeHeartbeat erNodeHeartbeat = new ErNodeHeartbeat();
        erNodeHeartbeat.setId(1);
        ErServerNode erServerNode = new ErServerNode();
        erServerNode.setId(1L);
        erServerNode.setEndpoint(new ErEndpoint("127.0.1.2", 8123));
        erServerNode.setName("unitTest");
        erServerNode.setStatus(ServerNodeStatus.HEALTHY.name());
        erServerNode.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        erNodeHeartbeat.setNode(erServerNode);
        Context context = new Context();
        erNodeHeartbeat = clusterManagerService.nodeHeartbeat(context, erNodeHeartbeat);
        System.out.println("erNodeHeartbeat = " + erNodeHeartbeat);

    }


}
