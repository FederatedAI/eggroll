package com.webank.eggroll.webapplication.model;



import com.google.inject.servlet.ServletModule;
import com.webank.eggroll.webapplication.servlet.MyServlet;

public class MyWebModule extends ServletModule{

    @Override
    protected void configureServlets() {
        super.configureServlets();


        //在这里可以完成完成加一些url绑定到我们的servlet上
//        serve("/myServlet").with(MyServlet.class) ;
    }
}
