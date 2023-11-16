package org.fedai.eggroll.webapp;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.webank.eggroll.webapp.controller.DispatcherServlet;
import com.webank.eggroll.webapp.controller.EggrollServiceProvider;
import com.webank.eggroll.webapp.controller.UserController;
import com.webank.eggroll.webapp.model.MyServletModule;
import org.apache.commons.cli.CommandLine;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.session.DefaultSessionCache;
import org.eclipse.jetty.server.session.DefaultSessionIdManager;
import org.eclipse.jetty.server.session.NullSessionDataStore;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.utils.CommandArgsUtils;
import org.fedai.eggroll.core.utils.PropertiesUtil;
import org.fedai.eggroll.webapp.controller.DispatcherServlet;
import org.fedai.eggroll.webapp.controller.EggrollServiceProvider;
import org.fedai.eggroll.webapp.model.MyServletModule;

import javax.servlet.DispatcherType;
import java.io.File;
import java.util.EnumSet;
import java.util.Properties;

public class JettyServer {

    public static void main(String[] args) throws Exception {
        System.setProperty("module", "cluster-manager");
        CommandLine cm = CommandArgsUtils.parseArgs(args);
        String confPath = cm.getOptionValue('c', "./conf/eggroll.properties");
        File file = new File(confPath);
        String absolutePath = file.getAbsolutePath();
        MetaInfo.STATIC_CONF_PATH = absolutePath;
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);

        Injector injector = Guice.createInjector(new MyServletModule());
        Server server = new Server(MetaInfo.JETTY_SERVER_PORT);
        SessionHandler sessionHandler = new SessionHandler();
        DefaultSessionCache sessionCache = new DefaultSessionCache(sessionHandler);
        DefaultSessionIdManager sessionIdManager = new DefaultSessionIdManager(server);
        server.setSessionIdManager(sessionIdManager);
        sessionCache.setSessionDataStore(new NullSessionDataStore());
        sessionHandler.setSessionCache(sessionCache);

        ServletContextHandler context = new ServletContextHandler();
        context.addEventListener(new GuiceServletContextListener() {
            @Override
            protected Injector getInjector() {
                return injector;
            }
        });

        // 注册并调用被@ApiMethod注解的方法
        DispatcherServlet apiMethodRegistry = injector.getInstance(DispatcherServlet.class);
        EggrollServiceProvider myService = injector.getInstance(EggrollServiceProvider.class);
        UserController userController = injector.getInstance(UserController.class);
        apiMethodRegistry.register(userController);
        apiMethodRegistry.register(myService);


        FilterHolder guiceFilter = context.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
        guiceFilter.setInitParameter("injectorFactory", "com.google.inject.servlet.GuiceServletContextListener");
        guiceFilter.setInitParameter("modules", MyServletModule.class.getName());
        // 设置SessionHandler为ContextHandler的处理程序
        context.setSessionHandler(sessionHandler);
        context.setContextPath("/");
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setResourceBase(JettyServer.class.getResource("/WEB-INF").toExternalForm());
        resourceHandler.setWelcomeFiles(new String[]{"index.html"});
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{resourceHandler, context});
        server.setHandler(handlers);
        server.start();
        server.join();
    }

}

