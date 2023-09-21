package com.webank.eggroll.webapp.model;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.webapp.controller.*;
import com.webank.eggroll.webapp.service.ZookeeperQueryService;

public class MyServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        super.configureServlets();
        this.install(new ClusterModule());
        // 绑定其他依赖类
        bind(ProcessorResourceController.class).in(Singleton.class);
        bind(ServerNodeController.class).in(Singleton.class);
        bind(NodeResourceController.class).in(Singleton.class);
        bind(SessionMainController.class).in(Singleton.class);
        bind(SessionProcessorController.class).in(Singleton.class);
        bind(ZookeeperQueryResource.class).in(Singleton.class);
//        // 绑定ZookeeperQueryService
//        bind(ZookeeperQueryService.class).toInstance(new ZookeeperQueryService("localhost:2181"));

        //配置url
        serve("/eggroll/processorresource").with(ProcessorResourceController.class);
        serve("/eggroll/servernode").with(ServerNodeController.class);
        serve("/eggroll/noderesource").with(NodeResourceController.class);
        serve("/eggroll/sessionmain").with(SessionMainController.class);
        serve("/eggroll/sessionprocessor").with(SessionProcessorController.class);
        serve("/eggroll/zookeeper-query").with(ZookeeperQueryResource.class);
    }
}
