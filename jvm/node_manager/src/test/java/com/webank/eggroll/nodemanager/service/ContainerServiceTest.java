package com.webank.eggroll.nodemanager.service;


import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.guice.module.NodeModule;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ContainerServiceTest {


    private static ContainerService containerService;

    @BeforeClass
    public static void before() {

        String confPath = "D:\\workspace\\FederatedAI\\eggroll\\jvm\\node_manager\\src\\main\\resources\\eggroll.properties";
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);

        Injector injector = Guice.createInjector(new NodeModule());
        containerService = injector.getInstance(ContainerService.class);


    }

    @Test
    public void testStartContainer() {
        ErSessionMeta sessionMeta = getParam();
        containerService.operateContainers(sessionMeta, Dict.NODE_CMD_START);
    }

    @Test
    public void testStopContainer() {
        ErSessionMeta sessionMeta = getParam();
        containerService.operateContainers(sessionMeta, Dict.NODE_CMD_START);
        containerService.operateContainers(sessionMeta,Dict.NODE_CMD_STOP);
    }

    @Test
    public void testKillContainer() {
        ErSessionMeta sessionMeta = getParam();
        containerService.operateContainers(sessionMeta, Dict.NODE_CMD_START);
        containerService.operateContainers(sessionMeta,Dict.NODE_CMD_KILL);
    }

    private ErSessionMeta getParam() {
        ErSessionMeta sessionMeta = new ErSessionMeta();
        List<ErProcessor> processors = new ArrayList<>();
        ErProcessor erProcessor = new ErProcessor();
        erProcessor.setName("erProcessor1");
        erProcessor.setSessionId("tasksessionId1");
        erProcessor.setProcessorType("egg_pair");
        erProcessor.setServerNodeId(-1L);

        processors.add(erProcessor);

        sessionMeta.setProcessors(processors);
        sessionMeta.setId("1111");
        sessionMeta.setName("sessionName");

        return sessionMeta;
    }

}