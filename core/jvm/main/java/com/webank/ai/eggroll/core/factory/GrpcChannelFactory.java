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

package com.webank.ai.eggroll.core.factory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.webank.ai.eggroll.core.retry.RetryException;
import com.webank.ai.eggroll.core.retry.Retryer;
import com.webank.ai.eggroll.core.retry.factory.AttemptOperations;
import com.webank.ai.eggroll.core.retry.factory.RetryerBuilder;
import com.webank.ai.eggroll.core.retry.factory.StopStrategies;
import com.webank.ai.eggroll.core.retry.factory.WaitTimeStrategies;
import com.webank.eggroll.core.constant.CoreConfKeys;
import com.webank.eggroll.core.constant.ModuleConstants;
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.model.Endpoint;
import com.webank.eggroll.core.session.DefaultEggrollConf;
import com.webank.eggroll.core.util.ThreadPoolUtils;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcChannelFactory {

  private LoadingCache<Endpoint, ManagedChannel> insecureChannelCache;
  private LoadingCache<Endpoint, ManagedChannel> secureChannelCache;

  private static final String channelWithBuckets = "[CHANNEL]";
  private static final String removeWithBuckets = "[REMOVE]";
  private static final String createWithBuckets = "[CREATE]";
  private static final String prefix = ModuleConstants.CORE_WITH_BRACKETS() + channelWithBuckets;

  private static final Logger LOGGER = LogManager.getLogger();

  public GrpcChannelFactory() {
    long maximumSize = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE(), 100);
    long expireTimeout = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC(), 1200);
    long channelTerminationAwaitTimeout = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC(), 20);

    CacheBuilder<Endpoint, ManagedChannel> cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(maximumSize)
        .expireAfterAccess(expireTimeout, TimeUnit.SECONDS)
        .recordStats()
        .weakValues()
        .removalListener(removalNotification -> {
          Endpoint endpoint = (Endpoint) removalNotification.getKey();
          ManagedChannel managedChannel = (ManagedChannel) removalNotification.getValue();
          StringBuilder removalPrefixBuilder = new StringBuilder()
              .append(prefix)
              .append(removeWithBuckets)
              .append(" removing for endpoint: ")
              .append(endpoint.toString())
              .append(". Reason: ")
              .append(removalNotification.getCause().name());

          if (managedChannel != null) {
            if (!managedChannel.isShutdown() || !managedChannel.isTerminated()) {
              managedChannel.shutdown();
            }

            try {
              if (managedChannel
                  .awaitTermination(channelTerminationAwaitTimeout, TimeUnit.SECONDS)) {
                LOGGER.debug("{}. Terminated.", removalPrefixBuilder);
              } else {
                LOGGER.debug("{} Await termination Timeout.", removalPrefixBuilder);
              }
            } catch (InterruptedException e) {
              LOGGER.debug("{}. Await termination interrupted.", removalPrefixBuilder);
            }
          } else {
            LOGGER.debug("{}. But channel is null.", removalPrefixBuilder);
          }
        });

    StringBuilder createPrefixBuilder = new StringBuilder()
        .append(prefix)
        .append(createWithBuckets);
    insecureChannelCache = cacheBuilder.build(new CacheLoader<Endpoint, ManagedChannel>() {
      @Override
      public ManagedChannel load(Endpoint endpoint) throws Exception {
        LOGGER.debug("{}[INSECURE] creating for endpoint: {}", createPrefixBuilder, endpoint);
        return createChannel(endpoint, false);
      }
    });

    secureChannelCache = cacheBuilder.build(new CacheLoader<Endpoint, ManagedChannel>() {
      @Override
      public ManagedChannel load(Endpoint endpoint) throws Exception {
        LOGGER.debug("{}[SECURE] creating for endpoint: {}", createPrefixBuilder, endpoint);
        return createChannel(endpoint, true);
      }
    });
  }

  private ManagedChannel createChannel(Endpoint endpoint, Boolean isSecureChannel) {
    long channelKeepAliveTimeSec = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC(), 300L);
    long channelKeepAliveTimeoutSec = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC(), 3600L);
    boolean channelKeepAliveWithoutCallsEnabled = DefaultEggrollConf
        .getBoolean(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED(), true);
    long channelIdleTimeoutSec = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC(), 3600);
    long channelPerRpcBufferLimit = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT(), 64 << 20);
    int channelFlowControlWindow = DefaultEggrollConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW(), 16 << 20);
    int channelMaxInboundMessageSize = DefaultEggrollConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE(), 32 << 20);
    long channelRetryBufferSize = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE(), 16 << 20);
    int channelMaxRetryAttempts = DefaultEggrollConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS(), 20);
    int channelExecutorPoolSize = DefaultEggrollConf
        .getInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE(), 100);
    String caCrtPath = DefaultEggrollConf
        .getString(CoreConfKeys.CONFKEY_CORE_SECURITY_CA_CRT_PATH(), StringConstants.EMPTY());

    File caCrt = null;
    if (isSecureChannel) {
      if (StringUtils.isBlank(caCrtPath)) {
        throw new IllegalArgumentException("secure channel required but no ca crt conf found");
      }

      caCrt = new File(caCrtPath);
      if (!caCrt.exists()) {
        throw new IllegalArgumentException("ca crt at path: " + caCrtPath + " not found");
      }
    }

    NettyChannelBuilder builder = NettyChannelBuilder
        .forAddress(endpoint.host(), endpoint.port())
        .executor(ThreadPoolUtils.defaultThreadPool())
        .keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS)
        .keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled)
        .idleTimeout(channelIdleTimeoutSec, TimeUnit.SECONDS)
        .perRpcBufferLimit(channelPerRpcBufferLimit)
        .flowControlWindow(channelFlowControlWindow)
        .maxInboundMetadataSize(channelMaxInboundMessageSize)
        .enableRetry()
        .retryBufferSize(channelRetryBufferSize)
        .maxRetryAttempts(channelMaxRetryAttempts);

    if (isSecureChannel) {
      SslContext sslContext = null;
      long sslSessionTimeout = DefaultEggrollConf
          .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC(), 3600 << 4);
      long sslSessionCacheSize = DefaultEggrollConf
          .getLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE(), 65536L);
      String keyCrtPath = DefaultEggrollConf
          .getString(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_CRT_PATH(), null);
      String keyPath = DefaultEggrollConf
          .getString(CoreConfKeys.CONFKEY_CORE_SECURITY_KEY_PATH(), null);

      try {
        SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient()
            .trustManager(caCrt)
            .sessionTimeout(sslSessionTimeout)
            .sessionCacheSize(sslSessionCacheSize);

        if (StringUtils.isNoneBlank(keyCrtPath, keyPath)) {
          File serverCrt = new File(keyCrtPath);
          File serverKey = new File(keyPath);

          sslContextBuilder.keyManager(serverCrt, serverKey);
        }
        sslContext = sslContextBuilder.build();
      } catch (SSLException e) {
        Thread.currentThread().interrupt();
        throw new SecurityException(e);
      }

      builder.sslContext(sslContext)
          .useTransportSecurity()
          .negotiationType(NegotiationType.TLS);
    } else {
      builder.negotiationType(NegotiationType.PLAINTEXT)
          .usePlaintext();
    }

    return builder.build();
  }

  private ManagedChannel getChannelInternal(Endpoint endpoint, boolean isSecureChannel) {
    ManagedChannel result = null;
    try {
      if (isSecureChannel) {
        result = secureChannelCache.get(endpoint);
      } else {
        result = insecureChannelCache.get(endpoint);
      }

      if (result.isShutdown() || result.isTerminated()) {
        insecureChannelCache.invalidate(result);
        result = insecureChannelCache.get(endpoint);
      }
    } catch (ExecutionException e) {
      LOGGER.error("error getting channel", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    return result;
  }

  public ManagedChannel getChannel(final Endpoint endpoint, boolean isSecureChannel) {
    ManagedChannel result = null;
    long fixedWaitTime = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS(), 1000L);
    int maxAttempts = DefaultEggrollConf
        .getInt(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS(), 10);
    long attemptTimeout = DefaultEggrollConf
        .getLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS(), 3000L);

    Retryer<ManagedChannel> retryer = RetryerBuilder.<ManagedChannel>newBuilder()
        .withWaitTimeStrategy(WaitTimeStrategies.fixedWaitTime(fixedWaitTime))
        .withStopStrategy(StopStrategies.stopAfterMaxAttempt(maxAttempts))
        .withAttemptOperation(
            AttemptOperations.<ManagedChannel>fixedTimeLimit(attemptTimeout, TimeUnit.MILLISECONDS))
        .retryIfAnyException()
        .build();

    final Callable<ManagedChannel> getUsableChannel = () -> getChannelInternal(endpoint,
        isSecureChannel);

    try {
      result = retryer.call(getUsableChannel);
    } catch (ExecutionException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RetryException e) {
      LOGGER.error("{} Error getting ManagedChannel after retries", prefix);
    }

    return result;
  }
}
