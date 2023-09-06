package com.webank.eggroll.webapplication.model;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.webapplication.controller.*;

public class MyServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        super.configureServlets();
        this.install(new ClusterModule());
        // 绑定 ProcessorResourceController
        bind(ProcessorResourceController.class).in(Singleton.class);
        bind(ServerNodeController.class).in(Singleton.class);
        bind(NodeResourceController.class).in(Singleton.class);
        bind(SessionMainController.class).in(Singleton.class);
        bind(SessionProcessorController.class).in(Singleton.class);
        // 绑定其他依赖类

        //配置url
        serve("/processorresource").with(ProcessorResourceController.class);
        serve("/servernode").with(ServerNodeController.class);
        serve("/noderesource").with(NodeResourceController.class);
        serve("/sessionmain").with(SessionMainController.class);
        serve("/sessionprocessor").with(SessionProcessorController.class);
    }
}
