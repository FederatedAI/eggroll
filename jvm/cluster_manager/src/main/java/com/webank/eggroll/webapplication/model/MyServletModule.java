package com.webank.eggroll.webapplication.model;

import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.webapplication.controller.ProcessorResourceController;
import com.webank.eggroll.webapplication.dao.ProcessorResourceDao;
import com.webank.eggroll.webapplication.service.ProcessorResourceService;

public class MyServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        // 绑定 ProcessorResourceController
        bind(ProcessorResourceController.class);

        // 绑定其他依赖类
        bind(ProcessorResourceDao.class).in(Singleton.class);
        bind(ProcessorResourceService.class).in(Singleton.class);

        // 配置 GuiceFilter
        filter("/*").through(GuiceFilter.class);


    }
}
