package com.webank.eggroll.webapp;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.webank.eggroll.webapp.model.MyServletModule;
import org.apache.commons.cli.CommandLine;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

import javax.servlet.DispatcherType;
import java.io.File;
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

//==========
        Injector injector = Guice.createInjector(new MyServletModule());
        Server server = new Server(8083);
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

        server.setHandler(context);
        server.start();
        server.join();

    }
}

