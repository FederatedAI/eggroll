package com.webank.eggroll.nodemanager.grpc;

import com.eggroll.core.invoke.InvokeInfo;
import com.eggroll.core.pojo.*;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import com.webank.eggroll.nodemanager.processor.DefaultProcessorManager;
import io.grpc.stub.StreamObserver;
import org.apache.ibatis.annotations.Param;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import static com.eggroll.core.grpc.CommandUri.*;

@Service
public class CommandServiceProvider extends CommandServiceGrpc.CommandServiceImplBase implements InitializingBean {

    Logger logger = LoggerFactory.getLogger(com.webank.eggroll.clustermanager.grpc.CommandServiceProvider.class);

    @Autowired
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
    public ErServerNode startContainers(ErServerNode erServerNode) {
        return null;
    }

    @URI(value = stopContainers)
    public ErServerNode stopContainers(ErServerNode erServerNode) {
        return null;
    }

    @URI(value = killContainers)
    public ErServerNode killContainers(ErServerNode erServerNode) {
        return null;
    }

    @URI(value = nodeHeartbeat)
    public ErServerNode nodeHeartbeat(ErServerNode erServerNode) {
        return null;
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        register(this);
        System.err.println("command  service provider afterPropertiesSet");

    }

    private void doRegister(String uri, Object service, Method method, Class paramClass) {
        InvokeInfo invokeInfo = new InvokeInfo(uri, service, method, paramClass);
        logger.info("register uri {}", invokeInfo);
        this.uriMap.put(uri, invokeInfo);
    }

    private void register(Object service) {
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
