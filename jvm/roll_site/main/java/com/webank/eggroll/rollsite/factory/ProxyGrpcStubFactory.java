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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.networking.proxy.DataTransferServiceGrpc;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.core.util.ToStringUtils;
import com.webank.eggroll.rollsite.channel.RedirectClientInterceptor;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import com.webank.eggroll.rollsite.security.SimpleTrustAllCertsManagerFactory;
import com.webank.eggroll.rollsite.service.FdnRouter;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.AbstractStub;
import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class ProxyGrpcStubFactory {
    private static final Logger LOGGER = LogManager.getLogger(ProxyGrpcStubFactory.class);
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private SimpleTrustAllCertsManagerFactory trustManagerFactory;
    @Autowired
    private FdnRouter fdnRouter;
    @Autowired
    private ProxyServerConf proxyServerConf;
    private LoadingCache<BasicMeta.Endpoint, ManagedChannel> channelCache;

    public ProxyGrpcStubFactory() {
        channelCache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .concurrencyLevel(50)
                .recordStats()
                .weakValues()
                .removalListener(removalNotification -> {
                    BasicMeta.Endpoint endpoint = (BasicMeta.Endpoint) removalNotification.getKey();
                    ManagedChannel managedChannel = (ManagedChannel) removalNotification.getValue();

                    LOGGER.trace("Managed channel removed for ip={}, port={}, hostname={}. reason={}",
                            endpoint.getIp(),
                            endpoint.getPort(),
                            endpoint.getHostname(),
                            removalNotification.getCause());
                    if (managedChannel != null) {
                        if (!managedChannel.isShutdown() || !managedChannel.isTerminated()) {
                            managedChannel.shutdown();
                        }
                    }
                })
                .build(new CacheLoader<BasicMeta.Endpoint, ManagedChannel>() {
                           @Override
                           public ManagedChannel load(BasicMeta.Endpoint endpoint) throws Exception {
                               Preconditions.checkNotNull(endpoint);
                               LOGGER.trace("creating channel for endpoint: ip={}, port={}, hostname={}",
                                       endpoint.getIp(), endpoint.getPort(), endpoint.getHostname());
                               return createChannel(endpoint);
                           }
                       }
                );
    }

    public long getChannelCacheSize() {
        return channelCache.size();
    }

    public DataTransferServiceGrpc.DataTransferServiceBlockingStub getBlockingStub(Proxy.Topic topic) {
        BasicMeta.Endpoint endpoint = fdnRouter.route(topic);

        return getBlockingStub(endpoint);
    }

    public DataTransferServiceGrpc.DataTransferServiceBlockingStub getBlockingStub(BasicMeta.Endpoint endpoint) {
        return (DataTransferServiceGrpc.DataTransferServiceBlockingStub) getStubBase(
                endpoint, false);
    }

    public DataTransferServiceGrpc.DataTransferServiceStub getAsyncStub(Proxy.Topic topic) {
        BasicMeta.Endpoint endpoint = fdnRouter.route(topic);

        return getAsyncStub(endpoint);
    }

    public DataTransferServiceGrpc.DataTransferServiceStub getAsyncStub(BasicMeta.Endpoint endpoint) {
        return (DataTransferServiceGrpc.DataTransferServiceStub) getStubBase(
                endpoint, true);
    }

    public BasicMeta.Endpoint getAsyncEndpoint(Proxy.Topic topic) {
        return fdnRouter.route(topic);
    }


    // todo:1: use retry framework
    private AbstractStub getStubBase(BasicMeta.Endpoint endpoint, boolean isAsync) {
        ManagedChannel managedChannel = null;

        for (int i = 0; i < 10; ++i) {
            try {
                managedChannel = getChannel(endpoint);
                if (managedChannel != null) {
                    break;
                }
            } catch (Exception e) {
                LOGGER.debug("get channel failed. target={}",
                        ToStringUtils.toOneLineString(endpoint), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
                channelCache.invalidate(endpoint);
            }
        }

        if (managedChannel == null) {
            throw new IllegalStateException("Error getting channel to " + ToStringUtils.toOneLineString(endpoint));
        }

        AbstractStub result;

        if (isAsync) {
            result = DataTransferServiceGrpc.newStub(managedChannel);
        } else {
            result = DataTransferServiceGrpc.newBlockingStub(managedChannel);
        }

        return result;
    }

    private ManagedChannel getChannel(BasicMeta.Endpoint endpoint) {
        ManagedChannel result = null;
        try {
            result = channelCache.get(endpoint);
            ConnectivityState state = result.getState(true);
            LOGGER.trace("Managed channel state: isShutdown: {}, isTerminated: {}, state: [}",
                    result.isShutdown(), result.isTerminated(), state.name());

            if (result.isShutdown() || result.isTerminated()) {
                channelCache.invalidate(result);
                result = channelCache.get(endpoint);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private ManagedChannel createChannel(BasicMeta.Endpoint endpoint) {
        String target = endpoint.getIp();
        if (Strings.isNullOrEmpty(target)) {
            target = endpoint.getHostname();
        }

        NettyChannelBuilder builder = NettyChannelBuilder
                .forAddress(target, endpoint.getPort())
                .executor((Executor) applicationContext.getBean("grpcClientExecutor"))
                .keepAliveTime(6, TimeUnit.MINUTES)
                .keepAliveTimeout(1, TimeUnit.DAYS)
                .keepAliveWithoutCalls(true)
                .idleTimeout(1, TimeUnit.DAYS)
                .perRpcBufferLimit(16 << 20)
                .flowControlWindow(32 << 20)
                .maxInboundMessageSize((2 << 30) - 1)
                .maxInboundMetadataSize(128 << 20)
                .enableRetry()
                .retryBufferSize(16 << 20)
                .maxRetryAttempts(20);      // todo:1: configurable

        if (proxyServerConf.isCompatibleEnabled()) {
            builder.intercept(new RedirectClientInterceptor("com.webank.ai.eggroll.api.rollstation.DataTransferService",
                    "com.webank.ai.fate.api.rollstation.DataTransferService"),
                    new RedirectClientInterceptor("com.webank.ai.eggroll.api.rollstation.RouteService",
                            "com.webank.ai.fate.api.rollstation.RouteService"));
        }

        // if secure client defined and endpoint is not in intranet
        if (proxyServerConf.isSecureClient() &&
                (!proxyServerConf.isNeighbourInsecureChannelEnabled() || !fdnRouter.isIntranet(endpoint))) {
            // todo:1: add configuration reading mechanism
            File caCrt = new File(proxyServerConf.getCaCrtPath());
            File serverCrt = new File(proxyServerConf.getServerCrtPath());
            File serverKey = new File(proxyServerConf.getServerKeyPath());

            SslContext sslContext = null;
            try {
                LOGGER.trace("use secure channel to {}", ToStringUtils.toOneLineString(endpoint));
                // sslContext = GrpcSslContexts.forClient().trustManager(trustManagerFactory).build();
                sslContext = GrpcSslContexts.forClient()
                        .trustManager(caCrt)
                        .keyManager(serverCrt, serverKey)
                        .sessionTimeout(3600 << 4)
                        .sessionCacheSize(65536)
                        .build();
            } catch (SSLException e) {
                throw new SecurityException(e);
            }
            builder.sslContext(sslContext).useTransportSecurity().negotiationType(NegotiationType.TLS);
        } else {
            LOGGER.trace("use insecure channel to {}", ToStringUtils.toOneLineString(endpoint));
            builder.negotiationType(NegotiationType.PLAINTEXT);
        }

        ManagedChannel managedChannel = builder
                .build();

        LOGGER.trace("created channel to {}", ToStringUtils.toOneLineString(endpoint));
        return managedChannel;
    }

}
