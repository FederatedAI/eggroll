package com.webank.eggroll.nodemanager;

import com.eggroll.core.boostrap.CommonBoostrap;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.ExtendEnvConf;
import com.eggroll.core.postprocessor.ApplicationStartedRunnerUtils;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.webank.eggroll.guice.module.NodeModule;
import com.webank.eggroll.nodemanager.meta.NodeManagerMeta;
import com.webank.eggroll.nodemanager.service.NodeResourceManager;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class Bootstrap {
    static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) throws Exception {
        CommonBoostrap.init(args, "node-manager");
        initDeepExtendConfig(args);
        Injector injector = Guice.createInjector(new NodeModule());
        List<String> packages = Lists.newArrayList();
        packages.add(Bootstrap.class.getPackage().getName());
        ApplicationStartedRunnerUtils.run(injector, packages, args);
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.info("==============shutdownHook===============");
                NodeResourceManager instance = injector.getInstance(NodeResourceManager.class);
                NodeManagerMeta.status = Dict.LOSS;
                instance.tryNodeHeartbeat();
            }
        });
        synchronized (injector) {
            try {
                injector.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void initDeepExtendConfig(String[] args) {
        logger.info("=============initDeepExtendConfig==============");
        CommandLine cmd = CommandArgsUtils.parseArgs(args);
        String confPath;
        if (cmd != null) {
            confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
        } else {
            confPath = "./conf/eggroll.properties";
        }
        String extendConfPath =confPath.replace("eggroll.properties","node-extend-env.properties");
        logger.info("load extend config file {}", extendConfPath);
        Properties extendPro = PropertiesUtil.getProperties(extendConfPath);
        ExtendEnvConf.initToMap(extendPro);
    }

}
