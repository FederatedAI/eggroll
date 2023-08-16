package com.webank.eggroll.clustermanager;


import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import org.apache.commons.cli.CommandLine;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Properties;

@MapperScan("com.webank.eggroll.clustermanager.dao.mapper")
@SpringBootApplication
@ConfigurationProperties
@EnableScheduling
public class Application {
    static Logger logger = LoggerFactory.getLogger(Application.class);

    public static ApplicationContext context;

    public static void main(String[] args) {
        System.setProperty("spring.config.name", "eggroll");
//        ClusterManagerBootstrap clusterManagerBootstrap = new ClusterManagerBootstrap();
        CommandLine cmd = CommandArgsUtils.parseArgs(args);

        //this.sessionId = cmd.getOptionValue('s')
        String confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);

        context = new SpringApplicationBuilder(Application.class).run(args);
        logger.debug("spring started !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        }));


//        ContextHolder.context_$eq(context);
//        clusterManagerBootstrap.init(args);
//        clusterManagerBootstrap.start();
        synchronized (context) {
            try {
                context.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}