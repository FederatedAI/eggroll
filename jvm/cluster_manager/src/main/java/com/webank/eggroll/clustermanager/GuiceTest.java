package com.webank.eggroll.clustermanager;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import com.webank.eggroll.guice.module.ClusterModule;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
public class GuiceTest {

    static  Logger  logger = LoggerFactory.getLogger(GuiceTest.class);

    public static  void main(String[] args){

//        Injector injector = Guice.createInjector(new DemoModule());
        Injector injector = Guice.createInjector(
                new ClusterModule());
        // Bootstrap the application by creating an instance of the server then
        // start the server to handle incoming requests.
      ServerNodeMapper  nodeMapper =  injector.getInstance(ServerNodeMapper.class);
        logger.info("{}",nodeMapper.selectById(1));
    }
}
