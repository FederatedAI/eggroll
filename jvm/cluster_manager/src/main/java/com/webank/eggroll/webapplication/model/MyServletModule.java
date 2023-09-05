package com.webank.eggroll.webapplication.model;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.webapplication.controller.ProcessorResourceController;
import com.webank.eggroll.webapplication.service.ProcessorResourceServiceN;
import com.webank.eggroll.webapplication.service.impl.ProcessorResourceServiceNImpl;

public class MyServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        super.configureServlets();
        // 绑定 ProcessorResourceController
        bind(ProcessorResourceController.class).in(Singleton.class);
        // 绑定其他依赖类
        bind(ProcessorResourceServiceN.class).to(ProcessorResourceServiceNImpl.class);
//        bind(ProcessorResourceMapper.class).to().in(Singleton.class);

        serve("/hello").with(ProcessorResourceController.class);
    }
}
