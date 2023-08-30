package com.webank.eggroll.clustermanager;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.FileSystemUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.spi.Element;
import com.google.inject.spi.Elements;
import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import com.webank.eggroll.guice.module.ClusterModule;
import org.apache.commons.cli.CommandLine;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.webank.eggroll.clustermanager.grpc.GrpcServer;

import java.util.Properties;

public class Bootstrap {

    static  Logger  logger = LoggerFactory.getLogger(Bootstrap.class);

    public static  void main(String[] args){
        CommandLine cmd = CommandArgsUtils.parseArgs(args);
        //this.sessionId = cmd.getOptionValue('s')
        String confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
        logger.info("load config file {}",confPath);
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);
//        Injector injector = Guice.createInjector(new DemoModule());
        Injector injector = Guice.createInjector(
                new ClusterModule());
        // Bootstrap the application by creating an instance of the server then
        // start the server to handle incoming requests.
//      ServerNodeMapper  nodeMapper =  injector.getInstance(ServerNodeMapper.class);
////        logger.info("{}",nodeMapper.selectById(1));
      //  System.err.println(injector.getAllBindings());

        GrpcServer  grpcServer = injector.getInstance(GrpcServer.class);
        try {
            grpcServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        synchronized (injector) {
            try {
                injector.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
