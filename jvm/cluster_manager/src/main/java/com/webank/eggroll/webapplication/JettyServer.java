package com.webank.eggroll.webapplication;

import com.google.inject.servlet.GuiceFilter;
//import com.webank.eggroll.webapplication.dao.ProcessorResourceDao;
import com.webank.eggroll.webapplication.model.MyServletModule;
import org.eclipse.jetty.server.Server;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

import javax.servlet.DispatcherType;
import java.util.EnumSet;

public class JettyServer {
    public static void main(String[] args) throws Exception {

        Injector injector = Guice.createInjector(new MyServletModule());
        Server server = new Server(8080);
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

