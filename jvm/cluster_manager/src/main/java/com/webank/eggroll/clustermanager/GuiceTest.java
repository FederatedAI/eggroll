package com.webank.eggroll.clustermanager;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import com.webank.eggroll.guice.module.ClusterModule;

public class GuiceTest {


    public static  void main(String[] args){

//        Injector injector = Guice.createInjector(new DemoModule());
        Injector injector = Guice.createInjector(
                new ClusterModule());
        // Bootstrap the application by creating an instance of the server then
        // start the server to handle incoming requests.
      ServerNodeMapper  nodeMapper =  injector.getInstance(ServerNodeMapper.class);
      System.err.println(nodeMapper.selectById(1));
    }
}
