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

package com.webank.ai.eggroll.framework;

import com.webank.ai.eggroll.core.api.grpc.access.AccessRedirector;
import com.webank.ai.eggroll.core.constant.StringConstants;
import com.webank.ai.eggroll.core.factory.DefaultGrpcServerFactory;
import com.webank.ai.eggroll.core.server.BaseEggRollServer;
import com.webank.ai.eggroll.core.server.DefaultServerConf;
import com.webank.ai.eggroll.framework.roll.api.grpc.server.RollKvServiceImpl;
import com.webank.ai.eggroll.framework.roll.api.grpc.server.RollProcessServiceImpl;
import com.webank.ai.eggroll.framework.roll.api.grpc.server.RollSessionServiceImpl;
import com.webank.ai.eggroll.framework.storage.service.server.ObjectStoreServicer;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import org.apache.commons.cli.CommandLine;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Roll extends BaseEggRollServer {
    public static void main(String[] args) throws Exception {
        String confFilePath = null;
        CommandLine cmd = parseArgs(args);

        if (cmd == null) {
            return;
        }

        confFilePath = cmd.getOptionValue("c");

        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext-roll.xml");

        DefaultGrpcServerFactory serverFactory = context.getBean(DefaultGrpcServerFactory.class);
        DefaultServerConf serverConf = (DefaultServerConf) serverFactory.parseConfFile(confFilePath);

        RollKvServiceImpl rollKvService = context.getBean(RollKvServiceImpl.class);
        ServerServiceDefinition rollKvServiceDefinition = ServerInterceptors
                .intercept(rollKvService, new ObjectStoreServicer.KvStoreInterceptor());
        RollProcessServiceImpl processService = context.getBean(RollProcessServiceImpl.class);
        RollSessionServiceImpl rollSessionService = context.getBean(RollSessionServiceImpl.class);

        serverConf
                .addService(rollKvServiceDefinition)
                .addService(processService)
                .addService(rollSessionService);

        boolean needCompatible = Boolean.valueOf(serverConf.getProperty(StringConstants.EGGROLL_COMPATIBLE_ENABLED, StringConstants.FALSE));

        if (needCompatible) {
            AccessRedirector accessRedirector = new AccessRedirector();

            serverConf.addService(accessRedirector.redirect(rollKvServiceDefinition,
                    "com.webank.ai.eggroll.api.storage.KVService",
                    "com.webank.ai.fate.api.eggroll.storage.KVService"))
                    .addService(accessRedirector.redirect(processService,
                            "com.webank.ai.eggroll.api.computing.processor.ProcessService",
                            "com.webank.ai.fate.api.eggroll.processor.ProcessService"));
        }

        Server server = serverFactory.createServer(serverConf);

        server.start();
        server.awaitTermination();
    }
}
