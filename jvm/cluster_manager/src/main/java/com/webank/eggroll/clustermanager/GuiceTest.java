package com.webank.eggroll.clustermanager;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class GuiceTest {


    public static  void main(String[] args){


        Injector injector = Guice.createInjector(
                new RequestLoggingModule(),
                new RequestHandlerModule(),
                new AuthenticationModule(),
                new DatabaseModule());
        // Bootstrap the application by creating an instance of the server then
        // start the server to handle incoming requests.
        injector.getInstance(MyWebServer.class)
                .start();
    }
}
