package com.webank.eggroll.webapp.model;

import com.eggroll.core.config.MetaInfo;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.clustermanager.register.ZooKeeperRegistration;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.webapp.controller.*;

public class MyServletModule extends ServletModule {

    private static final int port = MetaInfo.ZOOKEEPER_SERVER_PORT;
    private static final String host = MetaInfo.ZOOKEEPER_SERVER_HOST;
    @Override
    protected void configureServlets() {
        super.configureServlets();
        this.install(new ClusterModule());
        // 绑定其他依赖类
        bind(LoginController.class).in(Singleton.class);
        bind(DispatcherServlet.class).in(Singleton.class);
        bind(EggrollServiceProvider.class).in(Singleton.class);

        // 绑定ZookeeperQueryService,并从配置文件读取zk服务器地址，创建连接实例（获取zk服务器信息接口）
        String url =  ZooKeeperRegistration.generateZkUrl(host,port);

        //配置url映射
        //登录接口后期单独修改，目前没有登陆需求
        serve("/eggroll/login").with(LoginController.class);
        serve("/eggroll/*").with(DispatcherServlet.class);


    }
}
