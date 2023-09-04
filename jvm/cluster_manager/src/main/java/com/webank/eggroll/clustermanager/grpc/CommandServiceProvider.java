package com.webank.eggroll.clustermanager.grpc;

import com.eggroll.core.context.Context;
import com.eggroll.core.exceptions.EggRollBaseException;
import com.eggroll.core.exceptions.ErrorMessageUtil;
import com.eggroll.core.exceptions.ExceptionInfo;
import com.eggroll.core.flow.FlowLogUtil;
import com.eggroll.core.grpc.AbstractCommandServiceProvider;
import com.eggroll.core.grpc.Dispatcher;
import com.eggroll.core.grpc.URI;
import com.eggroll.core.invoke.InvokeInfo;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
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
public class CommandServiceProvider extends AbstractCommandServiceProvider {

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

    @Inject
    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.dispatcher.register(this);
    }

    @URI(value= nodeHeartbeat)
    public  ErNodeHeartbeat nodeHeartbeat(Context context ,ErNodeHeartbeat  erNodeHeartbeat){
        context.setNodeId(erNodeHeartbeat.getNode().getId().toString());
       return  clusterManagerService.nodeHeartbeat(context,erNodeHeartbeat);
    }

    @URI(value = getServerNode)
    public ErServerNode getServerNodeServiceName(Context context ,ErServerNode erServerNode) {
        logger.warn("==========>getServerNodeServiceName() , parameter {}", JsonUtil.object2Json(erServerNode));
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
        logger.warn("==========>getStore() , parameter {}", JsonUtil.object2Json(erStore));
        return storeCrudOperator.doGetStore(erStore);
    }

    @URI(value = getOrCreateStore)
    public ErStore getOrCreateStore(Context context ,ErStore erStore) {
        logger.warn("==========>getOrCreateStore() , parameter {}", JsonUtil.object2Json(erStore));
        return storeCrudOperator.doGetOrCreateStore(erStore);
    }

    @URI(value = deleteStore)
    public ErStore deleteStore(Context context ,ErStore erStore) {
        logger.warn("==========>deleteStore() , parameter {}", JsonUtil.object2Json(erStore));
        return storeCrudOperator.doDeleteStore(erStore);
    }

    @URI(value = getStoreFromNamespace)
    public ErStoreList getStoreFromNamespace(Context context ,ErStore erStore) {
        logger.warn("==========>getStoreFromNamespace() , parameter {}", JsonUtil.object2Json(erStore));
        return storeCrudOperator.getStoreFromNamespace(erStore);
    }

    @URI(value = getOrCreateSession)
    public ErSessionMeta getOrCreateSession(Context context ,ErSessionMeta sessionMeta) {
        logger.warn("==========>getOrCreateSession() , parameter {}", JsonUtil.object2Json(sessionMeta));
        return defaultSessionManager.getOrCreateSession(context, sessionMeta);
    }

    @URI(value = getSession)
    public ErSessionMeta getSession(Context context ,ErSessionMeta sessionMeta) {
        logger.warn("==========>getSession() , parameter {}", JsonUtil.object2Json(sessionMeta));
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
        logger.warn("==========>stopSession() , parameter {}", JsonUtil.object2Json(erSessionMeta));
        context.setSessionId(erSessionMeta.getId());
        return defaultSessionManager.stopSession(context, erSessionMeta);
    }

    @URI(value = killSession)
    public ErSessionMeta killSession(Context context ,ErSessionMeta erSessionMeta) {
        logger.warn("==========>killSession() , parameter {}", JsonUtil.object2Json(erSessionMeta));
        context.setSessionId(erSessionMeta.getId());
        return defaultSessionManager.killSession(context, erSessionMeta);
    }

    @URI(value = killAllSessions)
    public ErSessionMeta killAllSession(Context context ,ErSessionMeta erSessionMeta) {
        logger.warn("==========>killAllSession() , parameter {}", JsonUtil.object2Json(erSessionMeta));
        context.setSessionId(erSessionMeta.getId());
        return defaultSessionManager.killAllSessions(context, erSessionMeta);
    }


    @URI(value = submitJob)
    public SubmitJobResponse submitJob(Context context ,SubmitJobRequest request) throws InterruptedException {
        logger.warn("==========>submitJob() , parameter {}", JsonUtil.object2Json(request));
        return jobServiceHandler.handleSubmit(request);
    }

    @URI(value = queryJobStatus)
    public QueryJobStatusResponse queryJobStatus(Context context ,QueryJobStatusRequest request) throws InterruptedException {
        logger.warn("==========>queryJobStatus() , parameter {}", JsonUtil.object2Json(request));
        return jobServiceHandler.handleJobStatusQuery(request);
    }

    @URI(value = queryJob)
    public QueryJobResponse queryJob(Context context ,QueryJobRequest request) throws InterruptedException {
        logger.warn("==========>queryJob() , parameter {}", JsonUtil.object2Json(request));
        return jobServiceHandler.handleJobQuery(request);
    }

    @URI(value = killJob)
    public KillJobResponse killJob(Context context ,KillJobRequest request) throws InterruptedException {
        logger.warn("==========>killJob() , parameter {}", JsonUtil.object2Json(request));
        return jobServiceHandler.handleJobKill(request);
    }

    @URI(value = stopJob)
    public StopJobResponse stopJob(Context context ,StopJobRequest request) throws InterruptedException {
        logger.warn("==========>stopJob() , parameter {}", JsonUtil.object2Json(request));
        return jobServiceHandler.handleJobStop(request);
    }


}
