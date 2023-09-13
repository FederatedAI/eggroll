package com.webank.eggroll.clustermanager.grpc;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.AbstractCommandServiceProvider;
import com.eggroll.core.grpc.Dispatcher;
import com.eggroll.core.grpc.URI;
import com.eggroll.core.pojo.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.StoreCrudOperator;
import com.webank.eggroll.clustermanager.job.JobServiceHandler;
import com.webank.eggroll.clustermanager.processor.DefaultProcessorManager;
import com.webank.eggroll.clustermanager.session.DefaultSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        context.putLogData(Dict.STATUS,erNodeHeartbeat.getNode().getStatus());
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
        return storeCrudOperator.doGetOrCreateStore(context,erStore);
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
        context.putLogData(Dict.STATUS,erProcessor.getStatus());
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
        return jobServiceHandler.handleJobKill(context,request);
    }

    @URI(value = stopJob)
    public StopJobResponse stopJob(Context context ,StopJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleJobStop(context,request);
    }


    @URI(value = downloadJob)
    public DownloadJobResponse downloadJob(Context context ,DownloadJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleJobDownload(context,request);
    }

    @URI(value = prepareJobDownload)
    public PrepareJobDownloadResponse prepareJobDownload(Context context ,PrepareJobDownloadRequest request) throws InterruptedException {
        return jobServiceHandler.prepareJobDownload(context,request);
    }

}
