package org.fedai.eggroll.webapp.model;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import org.fedai.eggroll.clustermanager.register.ZooKeeperRegistration;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.guice.module.ClusterModule;
import org.fedai.eggroll.webapp.controller.DispatcherServlet;
import org.fedai.eggroll.webapp.controller.EggrollServiceProvider;
import org.fedai.eggroll.webapp.controller.LoginController;

public class MyServletModule extends ServletModule {

    private static final int PORT = MetaInfo.ZOOKEEPER_SERVER_PORT;
    private static final String HOST = MetaInfo.ZOOKEEPER_SERVER_HOST;
    @Override
    protected void configureServlets() {
        super.configureServlets();
        this.install(new ClusterModule());
        // 绑定其他依赖类
        bind(LoginController.class).in(Singleton.class);
        bind(DispatcherServlet.class).in(Singleton.class);
        bind(EggrollServiceProvider.class).in(Singleton.class);

        // 绑定ZookeeperQueryService,并从配置文件读取zk服务器地址，创建连接实例（获取zk服务器信息接口）
        String url =  ZooKeeperRegistration.generateZkUrl(HOST,PORT);

        //配置url映射
        //登录接口后期单独修改，目前没有登陆需求
        serve("/eggroll/login").with(LoginController.class);
        serve("/eggroll/*").with(DispatcherServlet.class);


    }
}
