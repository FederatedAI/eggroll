package com.webank.eggroll.nodemanager;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.postprocessor.ApplicationStartedRunnerUtils;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.guice.module.NodeModule;
import com.webank.eggroll.nodemanager.grpc.CommandServiceProvider;
import com.webank.eggroll.nodemanager.grpc.GrpcServer;
import com.webank.eggroll.nodemanager.service.NodeResourceManager;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Bootstrap {
    static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) {
        CommandLine cmd = CommandArgsUtils.parseArgs(args);
        String confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
        Properties environment = PropertiesUtil.getProperties(confPath);
        File file = new File(confPath);
        String absolutePath = file.getAbsolutePath();
        MetaInfo.STATIC_CONF_PATH = absolutePath;
        MetaInfo.init(environment);
        Injector injector = Guice.createInjector(new NodeModule());
        GrpcServer  grpcServer = injector.getInstance(GrpcServer.class);
        NodeResourceManager nodeResource = injector.getInstance(NodeResourceManager.class);
        CommandServiceProvider  commandServiceProvider= injector.getInstance(CommandServiceProvider.class);
        List<String> packages = Lists.newArrayList();
        packages.add(Bootstrap.class.getPackage().getName());
        try {
            ApplicationStartedRunnerUtils.run(injector, packages, args);
        } catch (Exception e) {
            logger.error("init error",e);
            //throw new RuntimeException(e);

        }


        try {
           // commandServiceProvider.register(commandServiceProvider);
            logger.info("============ register grpc server ===============");

            nodeResource.start();
            logger.info("============= start schedule task ==============");

            grpcServer.start();
            logger.info("============= node manager grpcServer is start ============");

        } catch (Exception e) {
            e.printStackTrace();
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
