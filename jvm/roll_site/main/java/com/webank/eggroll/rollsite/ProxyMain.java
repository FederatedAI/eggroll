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

package com.webank.eggroll.rollsite;

import com.webank.eggroll.core.session.StaticErConf;
import com.webank.eggroll.rollsite.factory.GrpcServerFactory;
import com.webank.eggroll.rollsite.factory.LocalBeanFactory;
import com.webank.eggroll.rollsite.manager.ServerConfManager;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import io.grpc.Server;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.ArrayList;

public class ProxyMain {
  private static final Logger LOGGER = LogManager.getLogger();

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    Option config = Option.builder("c")
        .argName("file")
        .longOpt("config")
        .hasArg()
        .numberOfArgs(1)
        .required()
        .desc("configuration file")
        .build();

    options.addOption(config);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    String confFilePath = cmd.getOptionValue("c");
    StaticErConf.addProperties(confFilePath);

    ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext-proxy.xml");

    LocalBeanFactory localBeanFactory = context.getBean(LocalBeanFactory.class);
    localBeanFactory.setApplicationContext(context);
    GrpcServerFactory serverFactory = context.getBean(GrpcServerFactory.class);

    ArrayList<Server> servers = serverFactory.createServers(confFilePath);

    ServerConfManager serverConfManager = context.getBean(ServerConfManager.class);
    ProxyServerConf proxyServerConf = serverConfManager.getProxyServerConf();

    LOGGER.info("Server started listening on port={}", proxyServerConf.getPort());
    LOGGER.info("server conf={}", proxyServerConf);

    for (int i = 0; i < servers.size(); i++) {
      Server server = servers.get(i);
      server.start();
    }

    for (int i = 0; i < servers.size(); i++) {
      Server server = servers.get(i);
      server.awaitTermination();
    }
  }
}
