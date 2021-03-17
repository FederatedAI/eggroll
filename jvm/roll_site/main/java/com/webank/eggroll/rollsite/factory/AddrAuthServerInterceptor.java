package com.webank.eggroll.rollsite.factory;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AddrAuthServerInterceptor implements ServerInterceptor {
    private static final Logger LOGGER = LogManager.getLogger();

    public static final Context.Key<Object> REMOTE_ADDR
            = Context.key("remoteAddr");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String inetSocketString = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();

        LOGGER.trace("inetSocketString={}, headers={}, methodNameDesctiptor={}, methodName={}", inetSocketString,
                headers, call.getMethodDescriptor(), call.getMethodDescriptor().getFullMethodName());

        String remoteAddr = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
        String[] remoteAddrSplited = remoteAddr.split(":");
        Context context = Context.current().withValue(REMOTE_ADDR, remoteAddrSplited[0].replaceAll("\\/", ""));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
