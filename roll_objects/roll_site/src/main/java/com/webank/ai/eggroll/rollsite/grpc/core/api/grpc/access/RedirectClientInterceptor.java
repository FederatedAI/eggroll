package com.webank.ai.eggroll.rollsite.grpc.core.api.grpc.access;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedirectClientInterceptor implements ClientInterceptor {
    private String existing;
    private String redirected;

    private static final Logger LOGGER = LogManager.getLogger();
    public RedirectClientInterceptor(String existing, String redirected) {
        this.existing = existing;
        this.redirected = redirected;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        String existingFullMethodName = method.getFullMethodName();
        MethodDescriptor<ReqT, RespT> redirectedMethod = method.toBuilder()
                .setFullMethodName(existingFullMethodName.replace(existing, redirected)).build();

        LOGGER.info("[PROXY] existing: {}, redirected: {}, redirectedMethod: {}, next channel: {}", existing, redirected, redirectedMethod.getFullMethodName(), next.authority());

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(redirectedMethod, callOptions)) {};
    }
}
