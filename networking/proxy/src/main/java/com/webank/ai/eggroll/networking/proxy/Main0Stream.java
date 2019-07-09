/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.ai.eggroll.networking.proxy;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.ai.eggroll.networking.proxy.factory.DefaultPipeFactory;
import com.webank.ai.eggroll.networking.proxy.factory.LocalBeanFactory;
import com.webank.ai.eggroll.networking.proxy.factory.PipeFactory;
import com.webank.ai.eggroll.networking.proxy.grpc.service.DataTransferPipedServerImpl;
import com.webank.ai.eggroll.networking.proxy.infra.Pipe;
import com.webank.ai.eggroll.networking.proxy.service.ConfFileBasedFdnRouter;
import com.webank.ai.eggroll.networking.proxy.service.FdnRouter;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.*;


public class Main0Stream {
    private static final Logger LOGGER = LogManager.getLogger(Main0Stream.class);

    public static void main(String[] args) throws Exception {

        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext-proxy.xml");
        LocalBeanFactory localBeanFactory = context.getBean(LocalBeanFactory.class);

        localBeanFactory.setApplicationContext(context);


        BasicMeta.Endpoint endpoint = BasicMeta.Endpoint.newBuilder().setIp("127.0.0.1").setPort(8888).build();
        Proxy.Metadata header = Proxy.Metadata.newBuilder()
                .setTask(Proxy.Task.newBuilder().setTaskId("123"))
                // .setDst(endpoint)
                // .setSrc(endpoint)
                .setOperator("operator")
                .build();

        // DataTransferServiceImpl dataTransferService = context.getBean(DataTransferServiceImpl.class);

        DataTransferPipedServerImpl dataTransferPipedServer =
                context.getBean(DataTransferPipedServerImpl.class);

        String routeTableFile = "src/main/resources/route_tables/route_table1.json";
        FdnRouter fdnRouter = context.getBean(ConfFileBasedFdnRouter.class);
        fdnRouter.setRouteTable(routeTableFile);

        InputStream is = new FileInputStream("/Users/max-webank/Downloads/software/StarUML-3.0.1.dmg");
        OutputStream os = new FileOutputStream("/tmp/testout");

        PipeFactory pipeFactory = context.getBean(DefaultPipeFactory.class);
        Pipe pipe =
                ((DefaultPipeFactory) pipeFactory).createInputStreamOutputStreamNoStoragePipe(is, os, header);

        // dataTransferPipedServer.setPipeFactory(pipeFactory);
        dataTransferPipedServer.setDefaultPipe(pipe);

        int port = 8888;

        // File crt = new File("src/main/resources/certs/server.crt");
        // File key = new File("src/main/resources/certs/server-private.pem");

        File crt = new File("/Users/max-webank/Documents/zmodem/127.0.0.1.crt");
        File key = new File("/Users/max-webank/Documents/zmodem/127.0.0.1-private.pem");

        System.out.println(crt.getAbsolutePath());
        Server server = ServerBuilder
                .forPort(port)
                .useTransportSecurity(crt, key)
                .addService(dataTransferPipedServer)
                .build()
                .start();

        LOGGER.info("Server started listening on port: {}", port);


        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                this.stop();
                System.err.println("*** server shut down");
            }
        });

        server.awaitTermination();
    }
}
