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
import com.webank.ai.eggroll.networking.proxy.factory.GrpcServerFactory;
import com.webank.ai.eggroll.networking.proxy.factory.LocalBeanFactory;
import com.webank.ai.eggroll.networking.proxy.factory.PipeFactory;
import com.webank.ai.eggroll.networking.proxy.infra.Pipe;
import com.webank.ai.eggroll.networking.proxy.model.ProxyServerConf;
import io.grpc.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class Main3 {
    private static final Logger LOGGER = LogManager.getLogger(Main3.class);

    public static void main(String[] args) throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext-proxy.xml");
        LocalBeanFactory localBeanFactory = context.getBean(LocalBeanFactory.class);
        localBeanFactory.setApplicationContext(context);
        GrpcServerFactory serverFactory = context.getBean(GrpcServerFactory.class);

        int port = 9999;

        ProxyServerConf proxyServerConf = context.getBean(ProxyServerConf.class);

        proxyServerConf.setSecureServer(true);
        proxyServerConf.setSecureClient(true);
        proxyServerConf.setRouteTablePath("src/main/resources/route_tables/route_table3.json");
        proxyServerConf.setPort(port);

        InputStream is = new FileInputStream("test.txt");
        OutputStream os = new FileOutputStream("/tmp/testout");
        // is = new ByteArrayInputStream("world".getBytes());

        BasicMeta.Endpoint endpoint = BasicMeta.Endpoint.newBuilder().setIp("127.0.0.1").setPort(8888).build();
        Proxy.Metadata header = Proxy.Metadata.newBuilder()
                .setTask(Proxy.Task.newBuilder().setTaskId("123"))
                //.setDst(endpoint)
                //.setSrc(endpoint)
                .setOperator("operator")
                .build();

        PipeFactory pipeFactory = context.getBean(DefaultPipeFactory.class);
        Pipe pipe =
                ((DefaultPipeFactory) pipeFactory).createInputStreamOutputStreamNoStoragePipe(is, os, header);
        proxyServerConf.setPipe(pipe);

        LOGGER.info("Server started listening on port: {}", port);

        Server server = serverFactory.createServer(proxyServerConf);

        LOGGER.info("server conf: {}", proxyServerConf);

        server.start();
        server.awaitTermination();
    }
}
