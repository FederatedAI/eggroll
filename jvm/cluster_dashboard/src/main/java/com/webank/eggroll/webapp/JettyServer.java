package com.webank.eggroll.webapp;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.webank.eggroll.clustermanager.session.SessionManager;
import com.webank.eggroll.webapp.model.MyServletModule;
import org.apache.commons.cli.CommandLine;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.session.DefaultSessionCache;
import org.eclipse.jetty.server.session.DefaultSessionIdManager;
import org.eclipse.jetty.server.session.NullSessionDataStore;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


import javax.servlet.DispatcherType;
import java.io.File;
import java.net.URL;
import java.util.EnumSet;
import java.util.Properties;

public class JettyServer {

    public static void main(String[] args) throws Exception {

        //MetaInfo init
        System.setProperty("module", "cluster-manager");
        CommandLine cm = CommandArgsUtils.parseArgs(args);
        String confPath = cm.getOptionValue('c', "./conf/eggroll.properties");
        File file = new File(confPath);
        String absolutePath = file.getAbsolutePath();
        MetaInfo.STATIC_CONF_PATH = absolutePath;
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);

        Injector injector = Guice.createInjector(new MyServletModule());
        Server server = new Server(8083);
        // 创建SessionHandler
        SessionHandler sessionHandler = new SessionHandler();
        // 创建一个默认的SessionCache
          DefaultSessionCache sessionCache = new DefaultSessionCache(sessionHandler);
        // 创建一个默认的SessionIdManager
        DefaultSessionIdManager sessionIdManager = new DefaultSessionIdManager(server);
        // 设置SessionIdManager
        server.setSessionIdManager(sessionIdManager);
        // 设置SessionCache
        sessionCache.setSessionDataStore(new NullSessionDataStore());
        // 设置SessionHandler的SessionCache
        sessionHandler.setSessionCache(sessionCache);

        ServletContextHandler context = new ServletContextHandler();
        context.addEventListener(new GuiceServletContextListener() {
            @Override
            protected Injector getInjector() {
                return injector;
            }
        });
        FilterHolder guiceFilter = context.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
        guiceFilter.setInitParameter("injectorFactory", "com.google.inject.servlet.GuiceServletContextListener");
        guiceFilter.setInitParameter("modules", MyServletModule.class.getName());
        // 设置SessionHandler为ContextHandler的处理程序
        context.setSessionHandler(sessionHandler);

        // 获取资源的绝对路径
//        ClassLoader loader = ClassLoader.getSystemClassLoader();
//        URL indexUrl = loader.getResource("index.html");
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL indexUrl = loader.getResource("com/webank/eggroll/webapp/index.html");
        String htmlLoc = "";
        if (indexUrl != null) {
            File indexFile = new File(indexUrl.getFile());
            htmlLoc = indexFile.getParentFile().getAbsolutePath();
            System.out.println("HTML Location: " + htmlLoc);
        } else {
            System.err.println("Resource 'index.html' not found.");
        }
        context.setResourceBase(htmlLoc);
        context.setWelcomeFiles(new String[] { "index.html" });
        context.addServlet(DefaultServlet.class, "/");

        server.setHandler(context);
        server.start();
        server.join();

    }
}

