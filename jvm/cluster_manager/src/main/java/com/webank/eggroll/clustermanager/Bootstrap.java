package com.webank.eggroll.clustermanager;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.postprocessor.ApplicationStartedRunnerUtils;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.clustermanager.grpc.GrpcServer;
import com.webank.eggroll.guice.module.ClusterModule;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Bootstrap {

    static Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    static public Injector injector;

    public static void main(String[] args) throws Exception {
        System.setProperty("module", "cluster-manager");
        CommandLine cmd = CommandArgsUtils.parseArgs(args);
        //this.sessionId = cmd.getOptionValue('s')
        String confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
        logger.info("load config file {}", confPath);
        File file = new File(confPath);
        String absolutePath = file.getAbsolutePath();
        MetaInfo.STATIC_CONF_PATH = absolutePath;
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);
//        Injector injector = Guice.createInjector(new DemoModule());
        injector = Guice.createInjector(
                new ClusterModule());
        // Bootstrap the application by creating an instance of the server then
        // start the server to handle incoming requests.
//      ServerNodeMapper  nodeMapper =  injector.getInstance(ServerNodeMapper.class);
////        logger.info("{}",nodeMapper.selectById(1));
        GrpcServer grpcServer = injector.getInstance(GrpcServer.class);
        List<String> packages = new ArrayList<>();
        packages.add(Bootstrap.class.getPackage().getName());
        ApplicationStartedRunnerUtils.run(injector, packages, args);


        synchronized (injector) {
            try {
                injector.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
