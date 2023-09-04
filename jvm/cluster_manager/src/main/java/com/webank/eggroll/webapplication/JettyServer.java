package com.webank.eggroll.webapplication;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.clustermanager.dao.impl.SessionRanksService;
import com.webank.eggroll.guice.module.ClusterModule;
import com.webank.eggroll.webapplication.controller.ProcessorResourceController;
//import com.webank.eggroll.webapplication.dao.ProcessorResourceDao;
import com.webank.eggroll.webapplication.model.MyGuiceServletContextListener;
import com.webank.eggroll.webapplication.model.MyServletModule;
import com.webank.eggroll.webapplication.model.MyWebModule;
import com.webank.eggroll.webapplication.service.ProcessorResourceService;
import com.webank.eggroll.webapplication.servlet.MyServlet;
import org.apache.commons.cli.CommandLine;
import org.eclipse.jetty.server.Server;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import java.util.Properties;

public class JettyServer {
    public static void main(String[] args) throws Exception {
//        CommandLine cmd = CommandArgsUtils.parseArgs(args);
//
//        //this.sessionId = cmd.getOptionValue('s')
//        String confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
//        Properties environment = PropertiesUtil.getProperties(confPath);
//        MetaInfo.init(environment);
//
//        Injector injector2 = Guice.createInjector(
//                new ClusterModule());

//        Injector injector = Guice.createInjector(new MyServletModule());
//        GuiceServletContextListener contextListener = new MyGuiceServletContextListener();
//        injector.injectMembers(contextListener);
//        Server server = new Server(8080);
//        WebAppContext webapp = new WebAppContext();
//        webapp.setContextPath("/");
//        webapp.addEventListener(new MyGuiceServletContextListener() {
//            @Override
//            protected Injector getInjector() {
//                return injector;
//            }
//        });
//        server.setHandler(webapp);
//
//        server.start();
//        server.join();

        // 创建Guice Injector，并将自定义的ServletContextListener添加到Jetty上下文中
        Injector injector = Guice.createInjector(new MyWebModule());
        GuiceServletContextListener contextListener = new MyGuiceServletContextListener();
        injector.injectMembers(contextListener);
        // 创建Jetty Server对象
        Server server = new Server(8080);
        // 创建ServletContextHandler，并为Jetty Server设置上下文路径
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        // 将Guice的ServletContextListener添加到ServletHandler中
        context.addEventListener(contextListener);
        // 添加Servlet映射：
        context.addServlet(new ServletHolder(new MyServlet()), "/hello/*");
        // 将ServletContextHandler设置到Jetty Server中
        server.setHandler(context);
        // 启动Jetty服务器
        server.start();
        server.join();
    }
}

