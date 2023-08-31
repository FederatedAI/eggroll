package com.webank.eggroll.nodemanager.grpc;

import com.eggroll.core.containers.meta.KillContainersResponse;
import com.eggroll.core.containers.meta.StartContainersResponse;
import com.eggroll.core.containers.meta.StopContainersResponse;
import com.eggroll.core.invoke.InvokeInfo;
import com.eggroll.core.pojo.*;
import com.eggroll.core.grpc.URI;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import com.webank.eggroll.nodemanager.processor.DefaultProcessorManager;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import static com.eggroll.core.grpc.CommandUri.*;

@Singleton
public class CommandServiceProvider extends CommandServiceGrpc.CommandServiceImplBase  {

    Logger logger = LoggerFactory.getLogger(CommandServiceProvider.class);

    @Inject
    DefaultProcessorManager defaultProcessorManager;


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
        InvokeInfo invokeInfo = uriMap.get(uri);
        logger.info("request {} invoke {}", uri, invokeInfo);
        if (invokeInfo == null) {
            throw new RuntimeException("invalid request : " + uri);
        }
        try {
            RpcMessage rpcMessage = (RpcMessage) invokeInfo.getParamClass().newInstance();
            rpcMessage.deserialize(data);
            RpcMessage response = (RpcMessage) invokeInfo.getMethod().invoke(invokeInfo.getObject(), rpcMessage);
            return response.serialize();

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;

    }

    @URI(value = startContainers)
    public ErSessionMeta startContainers(ErSessionMeta sessionMeta) {
        return defaultProcessorManager.startContainers(null, sessionMeta);
    }

    @URI(value = stopContainers)
    public ErSessionMeta stopContainers(ErSessionMeta sessionMeta) {
        return defaultProcessorManager.stopContainers(null, sessionMeta);
    }

    @URI(value = killContainers)
    public ErSessionMeta killContainers(ErSessionMeta sessionMeta) {
        return defaultProcessorManager.killContainers(null, sessionMeta);
    }

    @URI(value = eggpairHeartbeat)
    public ErProcessor heartbeat(ErProcessor processor) {
        return defaultProcessorManager.heartbeat(null, processor);
    }

    @URI(value = checkNodeProcess)
    public ErProcessor checkNodeProcess(ErProcessor processor) {
        return defaultProcessorManager.checkNodeProcess(null, processor);
    }

    @URI(value = startJobContainers)
    public StartContainersResponse startJobContainers(StartContainersRequest startContainersRequest) {
        return defaultProcessorManager.startJobContainers(startContainersRequest);
    }

    @URI(value = stopJobContainers)
    public StopContainersResponse stopJobContainers(StopContainersRequest stopContainersRequest) {
        return defaultProcessorManager.stopJobContainers(stopContainersRequest);
    }

    @URI(value = killJobContainers)
    public KillContainersResponse killJobContainers(KillContainersRequest killContainersRequest) {
        return defaultProcessorManager.killJobContainers(killContainersRequest);
    }


    @URI(value = nodeHeartbeat)
    public ErProcessor nodeHeartbeat(ErProcessor processor) {
        return null;
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
                    Class paramClass = types[0];
                    System.err.println("paramClass " + paramClass);
                    if (RpcMessage.class.isAssignableFrom(paramClass)) {
                        doRegister(uri.value(), service, method, paramClass);
                    } else {
//                       System.err.println("false "+paramClass);
                    }
                }

            }

        }

    }


}