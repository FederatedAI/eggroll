package org.fedai.eggroll.webapp.model;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import org.fedai.eggroll.clustermanager.register.ZooKeeperRegistration;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.guice.module.ClusterModule;
import org.fedai.eggroll.webapp.controller.DispatcherServlet;
import org.fedai.eggroll.webapp.controller.EggrollServiceProvider;
import org.fedai.eggroll.webapp.controller.UserController;
import org.fedai.eggroll.webapp.dao.service.RSASecurityService;
import org.fedai.eggroll.webapp.dao.service.SecurityService;
import org.fedai.eggroll.webapp.intercept.AuthenticationInterceptor;
import org.fedai.eggroll.webapp.intercept.UserInterceptor;


public class MyServletModule extends ServletModule {

    @Override
    protected void configureServlets() {
        super.configureServlets();
        this.install(new ClusterModule());
        this.install(new DashBoardModule());
        // 绑定其他依赖类
        bind(DispatcherServlet.class).in(Singleton.class);
        bind(EggrollServiceProvider.class).in(Singleton.class);
        bind(UserController.class).in(Singleton.class);
        bind(SecurityService.class).to(RSASecurityService.class);
        bind(UserInterceptor.class).to(AuthenticationInterceptor.class);

        //配置url映射
        serve("/eggroll/*").with(DispatcherServlet.class);


    }
}
