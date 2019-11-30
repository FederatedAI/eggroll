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

package com.webank.eggroll.rollsite.factory;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.rollsite.grpc.client.DataTransferPipedClient;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.manager.ServerConfManager;
import com.webank.eggroll.rollsite.service.FdnRouter;
import com.webank.eggroll.rollsite.channel.AccessRedirector;
import com.webank.eggroll.rollsite.grpc.service.DataTransferPipedServerImpl;
import com.webank.eggroll.rollsite.grpc.service.RouteServerImpl;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import com.google.gson.JsonObject;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


@Component
public class GrpcServerFactory {
    private static final Logger LOGGER = LogManager.getLogger(GrpcServerFactory.class);
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private ServerConfManager serverConfManager;
    @Autowired
    private FdnRouter fdnRouter;
    @Autowired
    private PipeFactory pipeFactory;
    @Autowired
    private DataTransferPipedServerImpl dataTransferPipedServer;
    @Autowired
    private RouteServerImpl routeServer;
    @Autowired
    private DefaultPipeFactory defaultPipeFactory;

    public Server createServer(ProxyServerConf proxyServerConf) {
        this.serverConfManager.setProxyServerConf(proxyServerConf);

        String routeTablePath = proxyServerConf.getRouteTablePath();
        //fdnRouter.setRouteTable(routeTablePath);

        if (proxyServerConf.getPipe() != null) {
            dataTransferPipedServer.setDefaultPipe(proxyServerConf.getPipe());
        } else if (proxyServerConf.getPipeFactory() != null) {
            dataTransferPipedServer.setPipeFactory(proxyServerConf.getPipeFactory());
        } else {
            dataTransferPipedServer.setPipeFactory(pipeFactory);
        }

        NettyServerBuilder serverBuilder = null;

        if (StringUtils.isBlank(proxyServerConf.getIp())) {
            LOGGER.info("server build on port only :{}", proxyServerConf.getPort());
            // LOGGER.warn("this may cause trouble in multiple network devices. you may want to consider binding to a ip");
            serverBuilder = NettyServerBuilder.forPort(proxyServerConf.getPort());
        } else {
            LOGGER.info("server build on address {}:{}", proxyServerConf.getIp(), proxyServerConf.getPort());
            InetSocketAddress inetSocketAddress = new InetSocketAddress(
                    InetAddresses.forString(proxyServerConf.getIp()), proxyServerConf.getPort());

            LOGGER.info(inetSocketAddress);
            SocketAddress addr =
                    new InetSocketAddress(
                            InetAddresses.forString(proxyServerConf.getIp()), proxyServerConf.getPort());
            serverBuilder = NettyServerBuilder.forAddress(addr);

        }

        serverBuilder.addService(dataTransferPipedServer)
                .addService(routeServer)
                .maxConcurrentCallsPerConnection(20000)
                .maxInboundMessageSize(32 << 20)
                .flowControlWindow(32 << 20)
                .keepAliveTime(6, TimeUnit.MINUTES)
                .keepAliveTimeout(24, TimeUnit.HOURS)
                .maxConnectionIdle(1, TimeUnit.HOURS)
                .permitKeepAliveTime(1, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .executor((TaskExecutor) applicationContext.getBean("grpcServiceExecutor"))
                .maxConnectionAge(24, TimeUnit.HOURS)
                .maxConnectionAgeGrace(24, TimeUnit.HOURS);

        if (proxyServerConf.isCompatibleEnabled()) {
            AccessRedirector accessRedirector = new AccessRedirector();

            serverBuilder.addService(accessRedirector.redirect(dataTransferPipedServer,
                    "com.webank.ai.eggroll.api.networking.proxy.DataTransferService",
                    "com.webank.ai.fate.api.networking.proxy.DataTransferService"))
                    .addService(accessRedirector.redirect(routeServer, "com.webank.ai.eggroll.api.networking.proxy.RouteService",
                            "com.webank.ai.fate.api.networking.proxy.RouteService"));
        }

        if (proxyServerConf.isSecureServer()) {
            String serverCrtPath = proxyServerConf.getServerCrtPath().replaceAll("\\.\\./", "");
            String serverKeyPath = proxyServerConf.getServerKeyPath().replaceAll("\\.\\./", "");
            String caCrtPath = proxyServerConf.getCaCrtPath().replaceAll("\\.\\./", "");

            File serverCrt = new File(serverCrtPath);
            File serverKey = new File(serverKeyPath);
            File caCrt = new File(caCrtPath);

            try {
                SslContextBuilder sslContextBuilder = GrpcSslContexts.forServer(serverCrt, serverKey)
                        .trustManager(caCrt);

                if (proxyServerConf.isNeighbourInsecureChannelEnabled()) {
                    sslContextBuilder.clientAuth(ClientAuth.OPTIONAL);
                } else {
                    sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
                }

                SslContext sslContext = sslContextBuilder
                        .sessionTimeout(3600 << 4)
                        .sessionCacheSize(65536)
                        .build();

                serverBuilder.sslContext(sslContext);
            } catch (SSLException e) {
                throw new SecurityException(e);
            }


            LOGGER.info("running in secure mode. server crt path: {}, server key path: {}, ca crt path: {}",
                    serverCrtPath, serverKeyPath, caCrtPath);
        } else {
            LOGGER.info("running in insecure mode");
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                this.stop();
                System.err.println("*** server shut down");
            }
        });


        //if(proxyServerConf.getRole().equals("host")) {
        //    initRouteTable(routeTablePath);
        //}
        fdnRouter.initRouteTableFile(routeTablePath);
        if(proxyServerConf.getRole().equals("guest")) {
            String srcIp = proxyServerConf.getIp();
            int srcPort = proxyServerConf.getPort();
            String srcPartyId = proxyServerConf.getPartyId();

            String dstIp = proxyServerConf.getGatewayIp();
            int dstPort = proxyServerConf.getGatewayPort();
            String dstPartyId = proxyServerConf.getGatewayPartyId();
            String dstRole = proxyServerConf.getGatewayRole();
            connecteToGateway(srcIp, srcPort, srcPartyId, "guest", dstIp, dstPort, dstPartyId, dstRole);

            fdnRouter.updateRouteTable(routeTablePath, dstPartyId, dstIp, dstPort);
        }

        fdnRouter.setRouteTable(routeTablePath);
        return serverBuilder.build();
    }

    public void connecteToGateway(String srcIp, int srcPort, String srcPartyId, String srcRole,
                                    String gatewayIp, int gatewayPort, String gatewayPartyId, String gatewayRole) {
        //DefaultPipeFactory defaultPipeFactory = new DefaultPipeFactory();
        String operator = "registerBroker";
        DataTransferPipedClient client = new DataTransferPipedClient();
        Proxy.Task.Builder taskBuilder = Proxy.Task.newBuilder();
        Proxy.Task task = taskBuilder.setTaskId("testTask").build();

        BasicMeta.Endpoint.Builder endPointBuilder = BasicMeta.Endpoint.newBuilder();

        System.out.println("gatewayPartyId:" + gatewayPartyId);
        Proxy.Topic.Builder topicBuilder = Proxy.Topic.newBuilder();
        Proxy.Topic topic1 = topicBuilder.setName("topic").setPartyId(gatewayPartyId).setRole(gatewayRole).build();
        Proxy.Topic topic2 = topicBuilder.setName("topic").setPartyId(srcPartyId).setRole(srcRole).build();

        Proxy.Metadata header = Proxy.Metadata.newBuilder()
            .setTask(task)
            .setDst(topic1)
            .setSrc(topic2)
            .setOperator(operator)
            .build();

        Proxy.Data data = Proxy.Data.newBuilder().setValue(ByteString.copyFromUtf8("hello")).build();

        Proxy.Packet packet = Proxy.Packet.newBuilder().setHeader(header).setBody(data).build();
        client.setEndpoint(BasicMeta.Endpoint.newBuilder().setIp(gatewayIp).setPort(gatewayPort).build());
        Proxy.Packet ret = client.unaryCall(packet, defaultPipeFactory.create("brokerInfo"));
        System.out.println("ret:" + ret.getBody().getValue());
    }

    public Server createServer(String confPath) throws IOException {
        ProxyServerConf proxyServerConf = applicationContext.getBean(ProxyServerConf.class);
        Properties properties = new Properties();

        Path absolutePath = Paths.get(confPath).toAbsolutePath();
        String finalConfPath = absolutePath.toString();

        try (InputStream is = new FileInputStream(finalConfPath)) {
            properties.load(is);

            String coordinator = properties.getProperty("coordinator", null);
            if (coordinator == null) {
                throw new IllegalArgumentException("coordinator cannot be null");
            } else {
                proxyServerConf.setCoordinator(coordinator);
            }

            String ipString = properties.getProperty("ip", null);
            proxyServerConf.setIp(ipString);

            String portString = properties.getProperty("port", null);
            if (portString == null) {
                throw new IllegalArgumentException("port cannot be null");
            } else {
                int port = Integer.valueOf(portString);
                proxyServerConf.setPort(port);
            }

            String partyIdString = properties.getProperty("partyId", null);
            if (partyIdString == null) {
                throw new IllegalArgumentException("partyId cannot be null");
            } else {
                //int partyId = Integer.valueOf(partyIdString);
                proxyServerConf.setPartyId(partyIdString);
            }

            String role = properties.getProperty("role", null);
            if (role == null) {
                throw new IllegalArgumentException("role cannot be null");
            } else {
                proxyServerConf.setRole(role);
            }

            System.out.println("role:" + role);
            if(role.equals("guest")) {
                String gatewayIpString = properties.getProperty("gatewayIp", null);
                proxyServerConf.setGatewayIp(gatewayIpString);

                String gatewayPortString = properties.getProperty("gatewayPort", null);
                if (gatewayPortString == null) {
                    throw new IllegalArgumentException("port cannot be null");
                } else {
                    int port = Integer.valueOf(gatewayPortString);
                    proxyServerConf.setGatewayPort(port);
                }

                System.out.println("set gatewayPartyId");
                String gatewayPartyIdString = properties.getProperty("gatewayPartyId", null);
                if (gatewayPartyIdString == null) {
                    throw new IllegalArgumentException("partyId cannot be null");
                } else {
                    proxyServerConf.setGatewayPartyId(gatewayPartyIdString);
                }

                String gatewayRoleString = properties.getProperty("gatewayRole", null);
                if (gatewayRoleString == null) {
                    throw new IllegalArgumentException("partyId cannot be null");
                } else {
                    proxyServerConf.setGatewayRole(gatewayRoleString);
                }
            }

            String routeTablePath = properties.getProperty("route.table", null);
            if (routeTablePath == null) {
                throw new IllegalArgumentException("route table cannot be null");
            } else {
                proxyServerConf.setRouteTablePath(routeTablePath);
            }

            boolean needCompatibility = Boolean.valueOf(properties.getProperty("eggroll.compatible.enabled", "false"));
            proxyServerConf.setCompatibleEnabled(needCompatibility);

            String serverCrt = properties.getProperty("server.crt");
            String serverKey = properties.getProperty("server.key");

            proxyServerConf.setServerCrtPath(serverCrt);
            proxyServerConf.setServerKeyPath(serverKey);

            if (StringUtils.isBlank(serverCrt) && StringUtils.isBlank(serverKey)) {
                proxyServerConf.setSecureServer(false);
            } else {
                proxyServerConf.setSecureServer(true);
            }

            String caCrt = properties.getProperty("ca.crt");
            proxyServerConf.setCaCrtPath(caCrt);

            if (StringUtils.isBlank(caCrt)) {
                proxyServerConf.setSecureClient(false);
            } else {
                proxyServerConf.setSecureClient(true);
            }

            String logPropertiesPath = properties.getProperty("log.properties");
            if (StringUtils.isNotBlank(logPropertiesPath)) {
                File logConfFile = new File(logPropertiesPath.replaceAll("\\.\\./", ""));
                if (logConfFile.exists() && logConfFile.isFile()) {
                    try {
                        ConfigurationSource configurationSource =
                                new ConfigurationSource(new FileInputStream(logConfFile), logConfFile);
                        Configurator.initialize(null, configurationSource);

                        proxyServerConf.setLogPropertiesPath(logPropertiesPath);
                        LOGGER.info("using log conf file: {}", logPropertiesPath);
                    } catch (Exception e) {
                        LOGGER.warn("failed to set log conf file at {}. using default conf", logPropertiesPath);
                    }
                }
            }

            String isAuditEnabled = properties.getProperty("audit.enabled");
            if (StringUtils.isNotBlank(isAuditEnabled)
                    && ("true".equals(isAuditEnabled.toLowerCase()))
                    || ("1".equals(isAuditEnabled))) {
                proxyServerConf.setAuditEnabled(true);
            } else {
                proxyServerConf.setAuditEnabled(false);
            }

            String isNeighbourInsecureChannelEnabled = properties.getProperty("neighbour.insecure.channel.enabled");
            if (StringUtils.isNotBlank(isNeighbourInsecureChannelEnabled)
                    && ("true".equals(isNeighbourInsecureChannelEnabled.toLowerCase()))
                    || ("1".equals(isNeighbourInsecureChannelEnabled))) {
                proxyServerConf.setNeighbourInsecureChannelEnabled(true);
            } else {
                proxyServerConf.setNeighbourInsecureChannelEnabled(false);
            }

            String isDebugEnabled = properties.getProperty("debug.enabled");
            if (StringUtils.isNotBlank(isDebugEnabled)
                    && ("true".equals(isDebugEnabled.toLowerCase()))
                    || ("1".equals(isDebugEnabled))) {
                proxyServerConf.setDebugEnabled(true);
            } else {
                proxyServerConf.setDebugEnabled(false);
            }
        } catch (Exception e) {
            LOGGER.error(e);
            throw e;
        }
        return createServer(proxyServerConf);
    }
}
