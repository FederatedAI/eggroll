package org.fedai.eggroll.nodemanager.service;


import com.google.inject.Guice;
import com.google.inject.Injector;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.*;
import org.fedai.eggroll.core.utils.PropertiesUtil;
import org.fedai.eggroll.guice.module.NodeModule;
import org.fedai.eggroll.nodemanager.processor.DefaultProcessorManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class ContainerServiceTest {

    private static ContainerService containerService;

    private static DefaultProcessorManager defaultProcessorManager;

    @BeforeClass
    public static void before() {

        String confPath = "D:\\workspace\\FederatedAI\\eggroll\\jvm\\node_manager\\src\\main\\resources\\eggroll.properties";
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);

        Injector injector = Guice.createInjector(new NodeModule());
        containerService = injector.getInstance(ContainerService.class);
        defaultProcessorManager = injector.getInstance(DefaultProcessorManager.class);

    }

    @Test
    public void testStartContainer() {
        ErSessionMeta sessionMeta = getParam();
        containerService.operateContainers(new Context(), sessionMeta, Dict.NODE_CMD_START);
    }

    @Test
    public void testStopContainer() {
        ErSessionMeta sessionMeta = getParam();
        containerService.operateContainers(new Context(), sessionMeta, Dict.NODE_CMD_START);
        containerService.operateContainers(new Context(), sessionMeta, Dict.NODE_CMD_STOP);
    }

    @Test
    public void testKillContainer() {
        ErSessionMeta sessionMeta = getParam();
        containerService.operateContainers(new Context(), sessionMeta, Dict.NODE_CMD_START);
        containerService.operateContainers(new Context(), sessionMeta, Dict.NODE_CMD_KILL);
    }


    @Test
    public void testStartJobContainers() {
        StartContainersRequest startContainersRequest = new StartContainersRequest();
        startContainersRequest.setJobType(JobProcessorTypes.DeepSpeed.getName());
        startContainersRequest.setSessionId("testSessionId");
        startContainersRequest.setName("testSessionName");

        DeepspeedContainerConfig deepspeedContainerConfig = new DeepspeedContainerConfig();
        deepspeedContainerConfig.setBackend("testBackend");
        deepspeedContainerConfig.setCrossRank(5);
        deepspeedContainerConfig.setStoreHost("localhost");
        deepspeedContainerConfig.setStorePort(3306);

        byte[] serialize = deepspeedContainerConfig.serialize();

        Map<Long, byte[]> typedExtraConfigs = new HashMap<>();
        typedExtraConfigs.put(1001L, serialize);
        startContainersRequest.setTypedExtraConfigs(typedExtraConfigs);

        defaultProcessorManager.startJobContainers(startContainersRequest);

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