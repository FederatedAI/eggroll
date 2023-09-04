package com.webank.eggroll.webapplication.model;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;

public class MyGuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
        return Guice.createInjector(new ServletModule() {
            @Override
            protected void configureServlets() {
//                serve("/*").with(MyServlet.class);
//                filter("/*").through(MyFilter.class);
            }
        });
    }
}
