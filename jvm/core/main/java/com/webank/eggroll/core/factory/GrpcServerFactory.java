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
 *
 *
 */

package com.webank.eggroll.core.factory;

import com.webank.eggroll.core.constant.CoreConfKeys;
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.ErEndpoint;
import com.webank.eggroll.core.session.StaticErConf;
import com.webank.eggroll.core.session.GrpcServerConf;
import com.webank.eggroll.core.util.FileSystemUtils;
import com.webank.eggroll.core.util.ThreadPoolUtils;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcServerFactory {
  private static final Logger LOGGER = LogManager.getLogger();

  private String prefix = "[CORE][SERVER]";

  public Server createServer(GrpcServerConf grpcServerConf) {
    NettyServerBuilder serverBuilder = null;

    ErEndpoint endpoint = grpcServerConf.endpoint();
    String host = endpoint.host();
    int port = endpoint.port();

    if (port <= 0) {
      throw new IllegalArgumentException(prefix + " cannot listen to port <= 0");
    }

    if (StringUtils.isBlank(host)) {
      LOGGER.debug("{} build on port only: {}", prefix, port);

      serverBuilder = NettyServerBuilder.forPort(port);
    } else {
      LOGGER.debug("{} build on address: {}:{}", prefix, host, port);
      SocketAddress addr = new InetSocketAddress(host, port);

      serverBuilder = NettyServerBuilder.forAddress(addr);
    }

    for (ServerServiceDefinition service : grpcServerConf.serverServiceDefinition()) {
      serverBuilder.addService(service);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        LOGGER.info("*** shutting down gRPC server in shutdown hook. addr: {} ***", endpoint);
        this.interrupt();
        LOGGER.info("*** server shut down. addr: {} ***", endpoint);
      }
    });

    int maxConcurrentCallPerConnection = StaticErConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONCURRENT_CALL_PER_CONNECTION(),
            10000);
    int maxInboundMessageSize = StaticErConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE(), 32 << 20);
    int flowControlWindow = StaticErConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_FLOW_CONTROL_WINDOW(), 16 << 20);
    long channelKeepAliveTimeSec = StaticErConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIME_SEC(), 300L);
    long channelKeepAliveTimeoutSec = StaticErConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC(), 3600L);
    long channelPermitKeepAliveTime = StaticErConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_PERMIT_KEEPALIVE_TIME_SEC(), 1L);
    boolean channelKeepAliveWithoutCallsEnabled = StaticErConf
        .getBoolean(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED(),
            true);
    long maxConnectionIdle = StaticErConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_IDLE_SEC(), 300L);
    long maxConnectionAge = StaticErConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_MAX_CONNECTION_AGE_SEC(), 86400L);
    long maxConnectionAgeGrace = StaticErConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_CONNECTION_AGE_GRACE_SEC(), 86400L);

    serverBuilder
        .executor(ThreadPoolUtils.defaultThreadPool())
        .maxConcurrentCallsPerConnection(maxConcurrentCallPerConnection)
        .maxInboundMessageSize(maxInboundMessageSize)
        .flowControlWindow(flowControlWindow)
        .keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS)
        .keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS)
        .permitKeepAliveTime(channelPermitKeepAliveTime, TimeUnit.SECONDS)
        .permitKeepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled)
        .maxConnectionIdle(maxConnectionIdle, TimeUnit.SECONDS)
        .maxConnectionAge(maxConnectionAge, TimeUnit.SECONDS)
        .maxConnectionAgeGrace(maxConnectionAgeGrace, TimeUnit.SECONDS);

    if (grpcServerConf.isSecureServer()) {
      String caCrtPath = FileSystemUtils.stripParentDirReference(StaticErConf
          .getString(CoreConfKeys.CONFKEY_CORE_SECURITY_CA_CRT_PATH(), StringConstants.EMPTY()));
      String keyCrtPath = FileSystemUtils.stripParentDirReference(StaticErConf
          .getString(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_CRT_PATH(), StringConstants.EMPTY()));
      String keyPath = FileSystemUtils.stripParentDirReference(StaticErConf
          .getString(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_PATH(), StringConstants.EMPTY()));
      boolean secureClusterEnabled = StaticErConf
          .getBoolean(CoreConfKeys.CONFKEY_CORE_SECURITY_SECURE_CLUSTER_ENABLED(), false);
      long sslSessionTimeout = StaticErConf
          .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_TIMEOUT_SEC(),
              3600 << 4);
      long sslSessionCacheSize = StaticErConf
          .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_SSL_SESSION_CACHE_SIZE(), 65536L);

      File caCrt = new File(caCrtPath);
      File keyCrt = new File(keyCrtPath);
      File key = new File(keyPath);

      try {
        SslContextBuilder sslContextBuilder = GrpcSslContexts.forServer(keyCrt, key)
            .trustManager(caCrt)
            .sessionTimeout(sslSessionTimeout)
            .sessionCacheSize(sslSessionCacheSize);

        if (secureClusterEnabled) {
          sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        } else {
          sslContextBuilder.clientAuth(ClientAuth.OPTIONAL);
        }

        serverBuilder.sslContext(sslContextBuilder.build());
      } catch (SSLException e) {
        Thread.currentThread().interrupt();
        throw new SecurityException(e);
      }

      LOGGER.debug(
          "{} starting in secure mode. server key path: {}, key crt path: {}, ca crt path: {}",
          prefix, keyPath, keyCrtPath, caCrtPath);
    } else {
      LOGGER.debug("{} starting in insecure mode.", prefix);
    }

    return serverBuilder.build();
  }
}
