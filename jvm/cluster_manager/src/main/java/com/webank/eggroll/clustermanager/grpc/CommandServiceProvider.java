package com.webank.eggroll.clustermanager.grpc;

import com.eggroll.core.context.Context;
import com.eggroll.core.exceptions.EggRollBaseException;
import com.eggroll.core.exceptions.ErrorMessageUtil;
import com.eggroll.core.exceptions.ExceptionInfo;
import com.eggroll.core.flow.FlowLogUtil;
import com.eggroll.core.grpc.URI;
import com.eggroll.core.invoke.InvokeInfo;
import com.eggroll.core.pojo.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.StoreCrudOperator;
import com.webank.eggroll.clustermanager.job.JobServiceHandler;
import com.webank.eggroll.clustermanager.processor.DefaultProcessorManager;
import com.webank.eggroll.clustermanager.session.DefaultSessionManager;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.eggroll.core.grpc.CommandUri.*;

@Singleton
public class CommandServiceProvider extends CommandServiceGrpc.CommandServiceImplBase{

    Logger logger = LoggerFactory.getLogger(CommandServiceProvider.class);

    @Inject
    DefaultSessionManager defaultSessionManager;
    @Inject
    DefaultProcessorManager defaultProcessorManager;
    @Inject
    ServerNodeService serverNodeService;
    @Inject
    StoreCrudOperator storeCrudOperator;
    @Inject
    ClusterManagerService  clusterManagerService;
    @Inject
    JobServiceHandler jobServiceHandler;


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

    ConcurrentHashMap<String, InvokeInfo> uriMap = new ConcurrentHashMap();

    public byte[] dispatch(String uri, byte[] data) {
        Context context  =new Context();
        context.setActionType(uri);
        try {
            InvokeInfo invokeInfo = uriMap.get(uri);
//            logger.info("request {} invoke {}", uri, invokeInfo);
            if (invokeInfo == null) {
                throw new RuntimeException("invalid request : " + uri);
            }
            try {
                RpcMessage rpcMessage = (RpcMessage) invokeInfo.getParamClass().newInstance();
                rpcMessage.deserialize(data);
                context.setRequest(rpcMessage);
                RpcMessage response = (RpcMessage) invokeInfo.getMethod().invoke(invokeInfo.getObject(), context, rpcMessage);
                return response.serialize();
            } catch (Exception e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
                ExceptionInfo exceptionInfo = ErrorMessageUtil.handleExceptionExceptionInfo(context, e);
                context.setReturnCode(exceptionInfo.getCode());
                context.setReturnMsg(exceptionInfo.getMessage());
                if(e instanceof  EggRollBaseException){
                    throw (EggRollBaseException)e;
                }else{
                    throw new RuntimeException(e);
                }
            }
        }finally {
            FlowLogUtil.printFlowLog(context);
        }

    }
    @URI(value= nodeHeartbeat)
    public  ErNodeHeartbeat nodeHeartbeat(Context context ,ErNodeHeartbeat  erNodeHeartbeat){
        context.setNodeId(erNodeHeartbeat.getNode().getId().toString());
       return  clusterManagerService.nodeHeartbeat(context,erNodeHeartbeat);
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
        context.setSessionId(erProcessor.getSessionId());
        context.setProcessorId(erProcessor.getId().toString());
        return defaultProcessorManager.heartbeat(context, erProcessor);
    }

    @URI(value = stopSession)
    public ErSessionMeta stopSession(Context context ,ErSessionMeta erSessionMeta) {
        context.setSessionId(erSessionMeta.getId());
        return defaultSessionManager.stopSession(context, erSessionMeta);
    }

    @URI(value = killSession)
    public ErSessionMeta killSession(Context context ,ErSessionMeta erSessionMeta) {
        context.setSessionId(erSessionMeta.getId());
        return defaultSessionManager.killSession(context, erSessionMeta);
    }

    @URI(value = killAllSessions)
    public ErSessionMeta killAllSession(Context context ,ErSessionMeta erSessionMeta) {
        context.setSessionId(erSessionMeta.getId());
        return defaultSessionManager.killAllSessions(context, erSessionMeta);
    }


    @URI(value = submitJob)
    public SubmitJobResponse submitJob(Context context ,SubmitJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleSubmit(request);
    }

    @URI(value = queryJobStatus)
    public QueryJobStatusResponse queryJobStatus(Context context ,QueryJobStatusRequest request) throws InterruptedException {
        return jobServiceHandler.handleJobStatusQuery(request);
    }

    @URI(value = queryJob)
    public QueryJobResponse queryJob(Context context ,QueryJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleJobQuery(request);
    }

    @URI(value = killJob)
    public KillJobResponse killJob(Context context ,KillJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleJobKill(request);
    }

    @URI(value = stopJob)
    public StopJobResponse stopJob(Context context ,StopJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleJobStop(request);
    }


    @Inject
    public void afterPropertiesSet(){
        register(this);
    }

    private void doRegister(String uri, Object service, Method method, Class paramClass) {
        InvokeInfo invokeInfo = new InvokeInfo(uri, service, method, paramClass);
        logger.info("register uri {}", invokeInfo);
        this.uriMap.put(uri, invokeInfo);
    }

    public  void register(Object service) {
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


}
