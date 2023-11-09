package com.webank.eggroll.nodemanager.grpc;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.grpc.ContextPrepareInterceptor;
import com.eggroll.core.grpc.ServiceExceptionHandler;
import com.eggroll.core.postprocessor.ApplicationStartedRunner;
import com.eggroll.core.utils.FileSystemUtils;
import com.eggroll.core.utils.NetUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.nodemanager.extend.NodeExtendTransferService;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Singleton
public class GrpcServer implements ApplicationStartedRunner {

    Logger logger = LoggerFactory.getLogger(GrpcServer.class);

    @Inject
    CommandServiceProvider commandServiceProvider;

    @Inject
    NodeExtendTransferService nodeExtendTransferService;

    public void start() throws Exception {
        Server server = createServer("0.0.0.0", MetaInfo.CONFKEY_NODE_MANAGER_PORT, Lists.newArrayList(commandServiceProvider, nodeExtendTransferService), Lists.newArrayList(), Maps.newHashMap());
        server.start();
    }

    public Server createServer(String host,
                               int port,
                               List<BindableService> grpcServices,
                               List<ServerServiceDefinition> bindServices,
                               Map<String, String> options) throws SSLException {


        if (port < 0) {
            throw new IllegalArgumentException("${modulePrefix} cannot listen to port <= 0");
        }
        if (grpcServices.isEmpty()) {
            throw new IllegalArgumentException("grpc services cannot be empty");
        }

        InetSocketAddress addr = new InetSocketAddress(host, port);

        NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forAddress(addr);

        grpcServices.forEach(s -> nettyServerBuilder.addService(ServerInterceptors.intercept(s, new ServiceExceptionHandler(), new ContextPrepareInterceptor())));
        bindServices.forEach(s -> nettyServerBuilder.addService(ServerInterceptors.intercept(s, new ServiceExceptionHandler(), new ContextPrepareInterceptor())));


        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("*** shutting down gRPC server in shutdown hook. host: {}, port: {} ***", NetUtils.getLocalHost(MetaInfo.CONFKEY_NODE_MANAGER_NET_DEVICE), MetaInfo.CONFKEY_NODE_MANAGER_PORT);
            }
        }));

        Integer maxConcurrentCallPerConnection = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION;
        Integer maxInboundMessageSize = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE;
        Integer maxInboundMetadataSize = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE;
        Integer flowControlWindow = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW;
        Integer channelKeepAliveTimeSec = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC;
        Integer channelKeepAliveTimeoutSec = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC;
        Integer channelPermitKeepAliveTime = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC;
        Boolean channelKeepAliveWithoutCallsEnabled = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED;
        Integer maxConnectionIdle = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC;
        Integer maxConnectionAge = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC;
        Integer maxConnectionAgeGrace = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(false).setNameFormat("GRPC-SERVER" + "-%d").build();

        final NettyServerBuilder nettyServerBuilder1 = nettyServerBuilder
                .executor(Executors.newCachedThreadPool(threadFactory))
                .maxConcurrentCallsPerConnection(maxConcurrentCallPerConnection)
                .maxInboundMessageSize(maxInboundMessageSize)
                .maxInboundMetadataSize(maxInboundMetadataSize)
                .flowControlWindow(flowControlWindow);

        if (channelKeepAliveTimeSec > 0) {
            nettyServerBuilder.keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS);
        }
        if (channelKeepAliveTimeoutSec > 0) {
            nettyServerBuilder.keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS);
        }
        if (channelPermitKeepAliveTime > 0) {
            nettyServerBuilder.permitKeepAliveTime(channelPermitKeepAliveTime, TimeUnit.SECONDS);
        }
        if (channelKeepAliveWithoutCallsEnabled) {
            nettyServerBuilder.permitKeepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled);
        }
        if (maxConnectionIdle > 0) {
            nettyServerBuilder.maxConnectionIdle(maxConnectionIdle, TimeUnit.SECONDS);
        }
        if (maxConnectionAge > 0) {
            nettyServerBuilder.maxConnectionAge(maxConnectionAge, TimeUnit.SECONDS);
        }
        if (maxConnectionAgeGrace > 0) {
            nettyServerBuilder.maxConnectionAgeGrace(maxConnectionAgeGrace, TimeUnit.SECONDS);
        }

        boolean secureClusterEnabled = MetaInfo.CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED;
        if (secureClusterEnabled) {
            String caCrtPath = FileSystemUtils.stripParentDirReference(MetaInfo.CONFKEY_CORE_SECURITY_CA_CRT_PATH);
            String keyCrtPath = FileSystemUtils.stripParentDirReference(MetaInfo.CONFKEY_CORE_SECURITY_KEY_CRT_PATH);
            String keyPath = FileSystemUtils.stripParentDirReference(MetaInfo.CONFKEY_CORE_SECURITY_KEY_PATH);

            Boolean clientAuthEnabled = MetaInfo.CONFKEY_CORE_SECURITY_CLIENT_AUTH_ENABLED;
            Integer sslSessionTimeout = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC;
            Integer sslSessionCacheSize = MetaInfo.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE;

            File caCrt = new File(caCrtPath);
            File keyCrt = new File(keyCrtPath);
            File key = new File(keyPath);

            SslContextBuilder sslContextBuilder = GrpcSslContexts.forServer(keyCrt, key).trustManager(caCrt).sessionTimeout(sslSessionTimeout)
                    .sessionCacheSize(sslSessionCacheSize);

            if (clientAuthEnabled) {
                sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
            } else {
                sslContextBuilder.clientAuth(ClientAuth.OPTIONAL);
            }

            nettyServerBuilder.sslContext(sslContextBuilder.build());

        } else {
            logger.info("gRPC server at {} starting in insecure mode", port);
        }
        return nettyServerBuilder.build();
    }


    @Override
    public void run(String[] args) throws Exception {
        start();
    }
}
