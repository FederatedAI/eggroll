package com.webank.ai.eggroll.framework.roll.api.grpc.client;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:applicationContext-roll.xml"})
public class TestEggSessionServiceClient {
    @Autowired
    private EggSessionServiceClient sessionServiceClient;
    @Autowired
    private ToStringUtils toStringUtils;

    private Node node;

    private BasicMeta.SessionInfo sessionInfo;

    public TestEggSessionServiceClient() {
        node = new Node();
        node.setIp("127.0.0.1");
        node.setPort(7888);

        sessionInfo = BasicMeta.SessionInfo.newBuilder()
                .setSessionId("test")
                .putComputingEngineConf("eggroll.computing.processor.venv", "/Users/max-webank/git/floss/python/venv/")
                .putComputingEngineConf("eggroll.computing.processor.python-path", "/Users/max-webank/git/")
                .build();
    }

    @Test
    public void testStartSession() {
        BasicMeta.SessionInfo result = sessionServiceClient.getOrCreateSession(sessionInfo, node);

        System.out.println(toStringUtils.toOneLineString(result));
    }

    @Test
    public void testStopSession() {
        BasicMeta.SessionInfo result = sessionServiceClient.stopSession(sessionInfo, node);

        System.out.println(toStringUtils.toOneLineString(result));
    }

    @Test
    public void testGetComputingEngine() {
        ComputingEngine result = sessionServiceClient.getComputingEngine(sessionInfo, node);
        System.out.println(toStringUtils.toOneLineString(result));
    }
}
