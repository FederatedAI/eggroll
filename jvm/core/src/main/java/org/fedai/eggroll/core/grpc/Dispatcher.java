package org.fedai.eggroll.core.grpc;

import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.ActionType;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.exceptions.EggRollBaseException;
import org.fedai.eggroll.core.exceptions.ErrorMessageUtil;
import org.fedai.eggroll.core.exceptions.ExceptionInfo;
import org.fedai.eggroll.core.flow.FlowLogUtil;
import org.fedai.eggroll.core.invoke.InvokeInfo;
import org.fedai.eggroll.core.pojo.RpcMessage;
import org.fedai.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class Dispatcher {
    Logger logger = LoggerFactory.getLogger(Dispatcher.class);
    Logger grpcLogger = LoggerFactory.getLogger("grpcTrace");

    public void call(Command.CommandRequest request,
                     StreamObserver<Command.CommandResponse> responseObserver) {
        String uri = request.getUri();
        byte[] resultBytes = dispatch(uri, request.getArgsList().get(0).toByteArray());
        Command.CommandResponse.Builder responseBuilder = Command.CommandResponse.newBuilder();
        responseBuilder.setId(request.getId());
        responseBuilder.addResults(ByteString.copyFrom(resultBytes));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }


    private ConcurrentHashMap<String, InvokeInfo> uriMap = new ConcurrentHashMap();

    public byte[] dispatch(String uri, byte[] data) {
        Context context = new Context();
        Object sourceIp = ContextPrepareInterceptor.sourceIp.get();
        if (sourceIp != null) {
            context.setSourceIp(sourceIp.toString());
        }
        context.setActionType(ActionType.SERVER.name());
        context.setUri(uri);
        try {
            InvokeInfo invokeInfo = uriMap.get(uri);
            if (invokeInfo == null) {
                logger.info("uri map {}", uriMap);
                throw new RuntimeException("invalid request : " + uri);
            }
            String traceId = uri + "_" + System.currentTimeMillis();
            try {
                RpcMessage rpcMessage = (RpcMessage) invokeInfo.getParamClass().newInstance();
                printGrpcTraceLog(traceId, "request", rpcMessage);
                rpcMessage.deserialize(data);
                context.setRequest(rpcMessage);
                RpcMessage response = (RpcMessage) invokeInfo.getMethod().invoke(invokeInfo.getObject(), context, rpcMessage);
                printGrpcTraceLog(traceId, "response", response);
                if (response != null) {
                    return response.serialize();
                }
                return new byte[0];
            } catch (Exception e) {
                ExceptionInfo exceptionInfo = ErrorMessageUtil.handleExceptionExceptionInfo(context, e);
                context.setReturnCode(exceptionInfo.getCode());
                context.setReturnMsg(exceptionInfo.getMessage());
                if (e.getCause() instanceof EggRollBaseException) {
                    throw (EggRollBaseException) e.getCause();
                } else {
                    throw new RuntimeException(e.getCause());
                }
            }
        } finally {
            FlowLogUtil.printFlowLog(context);
        }
    }


    private void doRegister(String uri, Object service, Method method, Class paramClass) {
        InvokeInfo invokeInfo = new InvokeInfo(uri, service, method, paramClass);
        logger.info("register uri {}", invokeInfo);
        this.uriMap.put(uri, invokeInfo);
    }

    public void register(Object service) {
        Method[] methods;

        if (service instanceof Class) {
            methods = ((Class) service).getMethods();
        } else {
            methods = service.getClass().getMethods();
        }
        for (Method method : methods) {

            URI uri = method.getDeclaredAnnotation(URI.class);

            if (uri != null) {
                Class[] types = method.getParameterTypes();
                if (types.length > 0) {
                    Class paramClass = types[1];

                    if (RpcMessage.class.isAssignableFrom(paramClass)) {
                        doRegister(uri.value(), service, method, paramClass);
                    } else {
//                       System.err.println("false "+paramClass);
                    }
                }

            }

        }

    }

    private void printGrpcTraceLog(String traceId, String type, RpcMessage message) {
        try {
            if (MetaInfo.EGGROLL_GRPC_REQUEST_PRINT) {
                grpcLogger.info("[{}_{}]====> {}", traceId, type, JsonUtil.object2Json(message));
            }
        } catch (Exception e) {
            logger.error("printGrpcTraceLog error : ", e);
        }
    }
}
