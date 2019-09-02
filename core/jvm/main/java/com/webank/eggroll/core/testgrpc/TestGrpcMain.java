/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
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

package com.webank.eggroll.core.testgrpc;

import com.webank.eggroll.core.di.Singletons;
import com.webank.eggroll.core.factory.GrpcServerFactory;
import com.webank.eggroll.core.session.GrpcServerConf;
import io.grpc.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestGrpcMain {

  private static final Logger LOGGER = LogManager.getLogger();

  public static void main(String[] args) throws Exception {
    GrpcServerFactory factory = Singletons.getNoCheck(GrpcServerFactory.class);
    GrpcServerConf serverConf = new GrpcServerConf("/tmp/test.properties");
    serverConf.setHost("localhost").setPort(50000);

    HelloServer helloServer = new HelloServer();

    serverConf.addService(helloServer);

    Server server = factory.createServer(serverConf);
    LOGGER.info("starting for endpoint: {}", serverConf.endpoint());
    server.start();
    server.awaitTermination();
  }
}
