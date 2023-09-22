package com.webank.eggroll.webapp.model;

import com.eggroll.core.config.MetaInfo;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.google.inject.servlet.SessionScoped;
import com.webank.eggroll.clustermanager.register.ZooKeeperRegistration;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.webapp.controller.*;
import com.webank.eggroll.webapp.service.ZookeeperQueryService;

import javax.servlet.http.HttpSessionListener;

public class MyServletModule extends ServletModule {

    private static final int port = MetaInfo.ZOOKEEPER_SERVER_PORT;
    private static final String host = MetaInfo.ZOOKEEPER_SERVER_HOST;
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
//        bind(ZookeeperQueryResource.class).in(Singleton.class);
        bind(LoginController.class).in(Singleton.class);
        bind(HttpSessionListener.class).to(MyHttpSessionListener.class);
        // 绑定ZookeeperQueryService,并从配置文件读取zk服务器地址，创建连接实例
        String url =  ZooKeeperRegistration.generateZkUrl(host,port);
//        bind(ZookeeperQueryService.class).toInstance(new ZookeeperQueryService(url)); //"localhost:2181"

        //配置url
        serve("/eggroll/processorresource").with(ProcessorResourceController.class);
        serve("/eggroll/servernode").with(ServerNodeController.class);
        serve("/eggroll/noderesource").with(NodeResourceController.class);
        serve("/eggroll/sessionmain").with(SessionMainController.class);
        serve("/eggroll/sessionprocessor").with(SessionProcessorController.class);
//        serve("/eggroll/zookeeper-query").with(ZookeeperQueryResource.class);
        serve("/eggroll/login").with(LoginController.class);
    }
}
