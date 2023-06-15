package com.webank.eggroll.clustermanager.bootstrap;


import com.webank.eggroll.core.command.CommandRouter;
import com.webank.eggroll.core.constant.MetadataCommands;
import com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@MapperScan("com.webank.ai.dao.mapper")
@EnableScheduling
public class Application {

    static ApplicationContext context  ;

    public static void main(String[] args) {

        ClusterManagerBootstrap  clusterManagerBootstrap = new ClusterManagerBootstrap();
        context=  new SpringApplicationBuilder(Application.class).run(args);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{

        }));
        clusterManagerBootstrap.start();
    }
}