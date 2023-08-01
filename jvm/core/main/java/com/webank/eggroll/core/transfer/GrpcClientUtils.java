//package com.webank.eggroll.core.transfer;
//
//import com.google.common.cache.CacheBuilder;
//import com.google.common.cache.RemovalNotification;
//import com.webank.eggroll.core.constant.CoreConfKeys;
//import com.webank.eggroll.core.constant.ModuleConstants;
//import com.webank.eggroll.core.meta.ErEndpoint;
//import com.webank.eggroll.core.retry.RetryException;
//import com.webank.eggroll.core.retry.Retryer;
//import com.webank.eggroll.core.retry.factory.RetryerBuilder;
//import com.webank.eggroll.core.retry.factory.StopStrategies;
//import com.webank.eggroll.core.retry.factory.WaitStrategies;
//import io.grpc.ManagedChannel;
//import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
//import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
//import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
//import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//
//public class GrpcClientUtils {
//    private static final long maximumSize = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_CACHE_SIZE.get());
//    private static final long expireTimeout = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_CACHE_EXPIRE_SEC.get());
//    private static final long channelTerminationAwaitTimeout = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_TERMINATION_AWAIT_TIMEOUT_SEC.get());
//    private static final CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
//            .maximumSize(maximumSize)
//            .expireAfterAccess(expireTimeout, TimeUnit.SECONDS)
//            .recordStats()
//            .removalListener((RemovalNotification<Object, Object> notification) -> {
//                ErEndpoint endpoint = (ErEndpoint) notification.getKey();
//                ManagedChannel managedChannel = (ManagedChannel) notification.getValue();
//                if (managedChannel != null && (!managedChannel.isShutdown() || !managedChannel.isTerminated())) {
//                    managedChannel.shutdown();
//                }
//                logDebug("[CHANNEL][REMOVAL] removing for endpoint=" + endpoint + ", id=" +
//                        Integer.toHexString(endpoint.hashCode()) + ". reason=" + notification.getCause().name());
//            });
//    private static final LoadingCache<ErEndpoint, ManagedChannel> insecureChannelCache =
//            cacheBuilder.build(new CacheLoader<ErEndpoint, ManagedChannel>() {
//                @Override
//                public ManagedChannel load(ErEndpoint endpoint) {
//                    logDebug("[CHANNEL][INSECURE] creating for endpoint=" + endpoint + ", id=" +
//                            Integer.toHexString(endpoint.hashCode()));
//                    return createChannel(endpoint, false);
//                }
//            });
//    private static final LoadingCache<ErEndpoint, ManagedChannel> secureChannelCache =
//            cacheBuilder.build(new CacheLoader<ErEndpoint, ManagedChannel>() {
//                @Override
//                public ManagedChannel load(ErEndpoint endpoint) {
//                    logDebug("[CHANNEL][SECURE] creating for endpoint=" + endpoint + ", id=" +
//                            Integer.toHexString(endpoint.hashCode()));
//                    return createChannel(endpoint, true);
//                }
//            });
//
//    private static final String channelWithBuckets = "[CHANNEL]";
//    private static final String removeWithBuckets = "[REMOVE]";
//    private static final String createWithBuckets = "[CREATE]";
//    private static final String prefix = ModuleConstants.CORE_WITH_BRACKETS + channelWithBuckets;
//
//    public static long getChannelCacheSize(boolean isSecure) {
//        return isSecure ? secureChannelCache.size() : insecureChannelCache.size();
//    }
//
//    private static ManagedChannel createChannel(ErEndpoint endpoint, boolean isSecureChannel) {
//        long channelKeepAliveTimeSec = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC.getWith(options));
//        long channelKeepAliveTimeoutSec = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIMEOUT_SEC.getWith(options));
//        boolean channelKeepAliveWithoutCallsEnabled = Boolean.parseBoolean(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED.getWith(options));
//        int channelIdleTimeoutSec = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_IDLE_TIMEOUT_SEC.getWith(options));
//        int channelPerRpcBufferLimit = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_PER_RPC_BUFFER_LIMIT.getWith(options));
//        int channelFlowControlWindow = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_FLOW_CONTROL_WINDOW.getWith(options));
//        int channelMaxInboundMessageSize = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.getWith(options));
//        int channelMaxInboundMetadataSize = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_INBOUND_METADATA_SIZE.getWith(options));
//        int channelRetryBufferSize = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_RETRY_BUFFER_SIZE.getWith(options));
//        int channelMaxRetryAttempts = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_MAX_RETRY_ATTEMPTS.getWith(options));
//        int channelExecutorPoolSize = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_EXECUTOR_POOL_SIZE.getWith(options));
//        String caCrtPath = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_CA_CRT_PATH.getWith(options);
//        File caCrt = null;
//        if (isSecureChannel) {
//            if (StringUtils.isBlank(caCrtPath)) {
//                throw new IllegalArgumentException("secure channel required but no ca crt conf found");
//            }
//            caCrt = new File(caCrtPath);
//            if (!caCrt.exists()) {
//                throw new IllegalArgumentException("ca crt at path: " + caCrtPath + " not found");
//            }
//        }
//        NettyChannelBuilder builder = NettyChannelBuilder
//                .forAddress(endpoint.host, endpoint.port)
//                .perRpcBufferLimit(channelPerRpcBufferLimit)
//                .flowControlWindow(channelFlowControlWindow)
//                .maxInboundMessageSize(channelMaxInboundMessageSize)
//                .maxInboundMetadataSize(channelMaxInboundMetadataSize);
//        if (channelIdleTimeoutSec > 0) {
//            builder.idleTimeout(channelIdleTimeoutSec, TimeUnit.SECONDS);
//        }
//        if (channelKeepAliveTimeSec > 0) {
//            builder.keepAliveTime(channelKeepAliveTimeSec, TimeUnit.SECONDS);
//        }
//        if (channelKeepAliveTimeoutSec > 0) {
//            builder.keepAliveTimeout(channelKeepAliveTimeoutSec, TimeUnit.SECONDS);
//        }
//        if (channelKeepAliveWithoutCallsEnabled) {
//            builder.keepAliveWithoutCalls(channelKeepAliveWithoutCallsEnabled);
//        }
//        if (channelMaxRetryAttempts > 0) {
//            builder.enableRetry()
//                    .retryBufferSize(channelRetryBufferSize)
//                    .maxRetryAttempts(channelMaxRetryAttempts);
//        } else {
//            builder.disableRetry();
//        }
//        if (isSecureChannel) {
//            SslContext sslContext = null;
//            long sslSessionTimeout = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_TIMEOUT_SEC.getWith(options));
//            long sslSessionCacheSize = Long.parseLong(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_SSL_SESSION_CACHE_SIZE.getWith(options));
//            String keyCrtPath = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_KEY_CRT_PATH.getWith(options);
//            String keyPath = CoreConfKeys.CONFKEY_CORE_SECURITY_CLIENT_KEY_PATH.getWith(options);
//            GrpcSslContexts.Builder sslContextBuilder = GrpcSslContexts
//                    .forClient()
//                    .trustManager(caCrt)
//                    .sessionTimeout(sslSessionTimeout)
//                    .sessionCacheSize(sslSessionCacheSize);
//            if (StringUtils.isNoneBlank(keyCrtPath, keyPath)) {
//                File serverCrt = new File(keyCrtPath);
//                File serverKey = new File(keyPath);
//                sslContextBuilder.keyManager(serverCrt, serverKey);
//            }
//            sslContext = sslContextBuilder.build();
//            builder.sslContext(sslContext)
//                    .useTransportSecurity()
//                    .negotiationType(NegotiationType.TLS);
//        } else {
//            builder.negotiationType(NegotiationType.PLAINTEXT)
//                    .usePlaintext();
//        }
//        return builder.build();
//    }
//
//    private static ManagedChannel getChannelInternal(ErEndpoint endpoint, boolean isSecureChannel) {
//        ManagedChannel result = null;
//        LoadingCache<ErEndpoint, ManagedChannel> cache = isSecureChannel ? secureChannelCache : insecureChannelCache;
//        result = cache.getUnchecked(endpoint);
//        if (result == null || result.isShutdown() || result.isTerminated()) {
//            if (isSecureChannel) {
//                cache.invalidate(result);
//            }
//            result = cache.get(endpoint);
//        }
//        return result;
//    }
//
//    public static ManagedChannel getChannel(ErEndpoint endpoint, boolean isSecureChannel, Map<String, String> options) {
//        ManagedChannel result = null;
//        long fixedWaitTime = Long.parseLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_WAIT_TIME_MS.getWith(options));
//        int maxAttempts = Integer.parseInt(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_MAX_ATTEMPTS.getWith(options));
//        long attemptTimeout = Long.parseLong(CoreConfKeys.CONFKEY_CORE_RETRY_DEFAULT_ATTEMPT_TIMEOUT_MS.getWith(options));
//        Retryer<ManagedChannel> retryer = RetryerBuilder.<ManagedChannel>newBuilder()
//                .withWaitStrategy(WaitStrategies.fixedWait(fixedWaitTime))
//                .withStopStrategy(StopStrategies.stopAfterAttempt(maxAttempts))
//                .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(attemptTimeout, TimeUnit.MILLISECONDS))
//                .retryIfException()
//                .build();
//        Callable<ManagedChannel> getUsableChannel = () -> getChannelInternal(endpoint, isSecureChannel);
//        try {
//            result = retryer.call(getUsableChannel);
//        } catch (ExecutionException e) {
//            Thread.currentThread().interrupt();
//            throw new RuntimeException(e);
//        } catch (RetryException e) {
//            logError("{} Error getting ManagedChannel after retries: " + prefix, e);
//            throw e;
//        }
//        return result;
//    }
//}