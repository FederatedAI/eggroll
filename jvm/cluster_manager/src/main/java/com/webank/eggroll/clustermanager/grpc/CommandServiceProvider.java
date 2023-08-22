package com.webank.eggroll.clustermanager.grpc;

import com.eggroll.core.context.Context;
import com.eggroll.core.flow.FlowLogUtil;
import com.eggroll.core.grpc.URI;
import com.eggroll.core.invoke.InvokeInfo;
import com.eggroll.core.pojo.*;
import com.google.protobuf.ByteString;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.StoreCrudOperator;
import com.webank.eggroll.clustermanager.processor.DefaultProcessorManager;
import com.webank.eggroll.clustermanager.session.DefaultSessionManager;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.eggroll.core.grpc.CommandUri.*;

@Service
public class CommandServiceProvider extends CommandServiceGrpc.CommandServiceImplBase implements InitializingBean {

    Logger logger = LoggerFactory.getLogger(CommandServiceProvider.class);

    @Autowired
    DefaultSessionManager defaultSessionManager;
    @Autowired
    DefaultProcessorManager defaultProcessorManager;
    @Autowired
    ServerNodeService serverNodeService;
    @Autowired
    StoreCrudOperator storeCrudOperator;
    @Autowired
    ClusterManagerService  clusterManagerService;


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
        Context context  =new Context();
        context.setActionType(uri);
        try {
            InvokeInfo invokeInfo = uriMap.get(uri);
            logger.info("request {} invoke {}", uri, invokeInfo);
            if (invokeInfo == null) {
                throw new RuntimeException("invalid request : " + uri);
            }
            try {
                RpcMessage rpcMessage = (RpcMessage) invokeInfo.getParamClass().newInstance();
                rpcMessage.deserialize(data);
                RpcMessage response = (RpcMessage) invokeInfo.getMethod().invoke(invokeInfo.getObject(), context, rpcMessage);
                return response.serialize();

            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }finally {
            FlowLogUtil.printFlowLog(context);
        }

    }
    @URI(value= nodeHeartbeat)
    public  ErNodeHeartbeat nodeHeartbeat(Context context ,ErNodeHeartbeat  erNodeHeartbeat){
       return  clusterManagerService.nodeHeartbeat(erNodeHeartbeat);
    }

    @URI(value = getServerNode)
    public ErServerNode getServerNodeServiceName(Context context ,ErServerNode erServerNode) {
        List<ErServerNode> nodeList = serverNodeService.getListByErServerNode(erServerNode);
        return nodeList.size() > 0 ? nodeList.get(0) : null;
    }

    @URI(value = getServerNodes)
    public ErServerCluster getServerNodesServiceName(Context context ,ErServerNode erServerNode) {
        return null;
    }

    @URI(value = getOrCreateServerNode)
    public ErServerNode getOrCreateServerNode(Context context ,ErServerNode erServerNode) {
        return null;
    }

    @URI(value = createOrUpdateServerNode)
    public ErServerNode createOrUpdateServerNode(Context context ,ErServerNode erServerNode) {
        return null;
    }

    @URI(value = getStore)
    public ErStore getStore(Context context ,ErStore erStore) {
        return storeCrudOperator.doGetStore(erStore);
    }

    @URI(value = getOrCreateStore)
    public ErStore getOrCreateStore(Context context ,ErStore erStore) {
        return storeCrudOperator.doGetOrCreateStore(erStore);
    }

    @URI(value = deleteStore)
    public ErStore deleteStore(Context context ,ErStore erStore) {
        return storeCrudOperator.doDeleteStore(erStore);
    }

    @URI(value = getStoreFromNamespace)
    public ErStoreList getStoreFromNamespace(Context context ,ErStore erStore) {
        return storeCrudOperator.getStoreFromNamespace(erStore);
    }

    @URI(value = getOrCreateSession)
    public ErSessionMeta getOrCreateSession(Context context ,ErSessionMeta sessionMeta) {

        return defaultSessionManager.getOrCreateSession(context, sessionMeta);
    }

    @URI(value = getSession)
    public ErSessionMeta getSession(Context context ,ErSessionMeta sessionMeta) {

        return defaultSessionManager.getSession(context, sessionMeta);
    }

    @URI(value = heartbeat)
    public ErProcessor heartbeat(Context context ,ErProcessor erProcessor) {

        return defaultProcessorManager.heartbeat(context, erProcessor);
    }

    @URI(value = stopSession)
    public ErSessionMeta stopSession(Context context ,ErSessionMeta erSessionMeta) {

        return defaultSessionManager.stopSession(context, erSessionMeta);
    }

    @URI(value = killSession)
    public ErSessionMeta killSession(Context context ,ErSessionMeta erSessionMeta) {

        return defaultSessionManager.killSession(context, erSessionMeta);
    }

    @URI(value = killAllSessions)
    public ErSessionMeta killAllSession(Context context ,ErSessionMeta erSessionMeta) {

        return defaultSessionManager.killAllSessions(context, erSessionMeta);
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
                    Class paramClass = types[1];
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
