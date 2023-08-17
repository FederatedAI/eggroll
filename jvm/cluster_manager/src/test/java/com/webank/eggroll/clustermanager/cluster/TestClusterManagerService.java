package com.webank.eggroll.clustermanager.cluster;

import com.eggroll.core.constant.ServerNodeStatus;
import com.eggroll.core.constant.ServerNodeTypes;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErNodeHeartbeat;
import com.eggroll.core.pojo.ErServerNode;
import com.webank.eggroll.clustermanager.Application;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest(classes = Application.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class TestClusterManagerService {


    @Autowired
    ClusterManagerService clusterManagerService;

    @Test
    public void testNodeHeartbeat(){
        ErNodeHeartbeat erNodeHeartbeat = new ErNodeHeartbeat();
        erNodeHeartbeat.setId(1);
        ErServerNode erServerNode = new ErServerNode();
        erServerNode.setId(1L);
        erServerNode.setEndpoint(new ErEndpoint("127.0.1.2",8123));
        erServerNode.setName("unitTest");
        erServerNode.setStatus(ServerNodeStatus.HEALTHY.name());
        erServerNode.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        erNodeHeartbeat.setNode(erServerNode);
        erNodeHeartbeat = clusterManagerService.nodeHeartbeat(erNodeHeartbeat);
        System.out.println("erNodeHeartbeat = " + erNodeHeartbeat);

    }



}
