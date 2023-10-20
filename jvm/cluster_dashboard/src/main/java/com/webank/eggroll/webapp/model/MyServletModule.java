package com.webank.eggroll.webapp.model;

import com.eggroll.core.config.MetaInfo;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.clustermanager.register.ZooKeeperRegistration;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.webapp.controller.*;
import org.eclipse.jetty.servlet.DefaultServlet;

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
        bind(LoginController.class).in(Singleton.class);


        // 集群接口的绑定
        bind(NodeSituationController.class).in(Singleton.class);
        bind(NodeDetailController.class).in(Singleton.class);
        bind(PrenodeSessionInfoController.class).in(Singleton.class);
        bind(QuerySessionProcessorController.class).in(Singleton.class);

        bind(DefaultServlet.class).in(Scopes.SINGLETON);

        // bind(ZookeeperQueryResource.class).in(Singleton.class);
        // 绑定ZookeeperQueryService,并从配置文件读取zk服务器地址，创建连接实例
        String url =  ZooKeeperRegistration.generateZkUrl(host,port);
        //bind(ZookeeperQueryService.class).toInstance(new ZookeeperQueryService(url)); //"localhost:2181"

        //配置url
        serve("/eggroll/processorresource").with(ProcessorResourceController.class);
        serve("/eggroll/servernode").with(ServerNodeController.class);
        serve("/eggroll/noderesource").with(NodeResourceController.class);
        serve("/eggroll/sessionmain").with(SessionMainController.class);
        serve("/eggroll/sessionprocessor").with(SessionProcessorController.class);
        //serve("/eggroll/zookeeper-query").with(ZookeeperQueryResource.class);
        serve("/eggroll/login").with(LoginController.class);

        // 集群接口的配置
        serve("/eggroll/nodesituation").with(NodeSituationController.class);
        serve("/eggroll/nodedetail").with(NodeDetailController.class);
        serve("/eggroll/prenodesessioninfo").with(PrenodeSessionInfoController.class);
        serve("/eggroll/querysessionprocessor").with(QuerySessionProcessorController.class);


        serve("/static/*").with(DefaultServlet.class);

    }
}
