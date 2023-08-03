package com.webank.eggroll.nodemanager;

//import com.webank.eggroll.clustermanager.grpc.GrpcServer;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.CommandArgsUtils;
import com.eggroll.core.utils.PropertiesUtil;
import com.webank.eggroll.nodemanager.grpc.GrpcServer;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import sun.tools.jar.CommandLine;

import java.util.Properties;

@SpringBootApplication
@ConfigurationProperties
public class Application {

    static Logger logger = LoggerFactory.getLogger(Application.class);

    public static ApplicationContext context  ;

    public static void main(String[] args) {
        System.setProperty("spring.config.name","eggroll");
        CommandLine cmd = CommandArgsUtils.parseArgs(args);
        String confPath = cmd.getOptionValue('c', "./conf/eggroll.properties");
        Properties environment = PropertiesUtil.getProperties(confPath);
        MetaInfo.init(environment);

        context=  new SpringApplicationBuilder(Application.class).run(args);

        GrpcServer grpcServer = context.getBean("grpcServer");
        try {
            grpcServer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        synchronized(context) {
            try {
                context.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
