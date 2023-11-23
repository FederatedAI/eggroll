package org.fedai.eggroll.clustermanager.grpc;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.cluster.ClusterManagerService;
import org.fedai.eggroll.clustermanager.cluster.ClusterResourceManager;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.StoreCrudOperator;
import org.fedai.eggroll.clustermanager.job.JobServiceHandler;
import org.fedai.eggroll.clustermanager.processor.DefaultProcessorManager;
import org.fedai.eggroll.clustermanager.session.DefaultSessionManager;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.StatusReason;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.deepspeed.store.*;
import org.fedai.eggroll.core.grpc.AbstractCommandServiceProvider;
import org.fedai.eggroll.core.grpc.Dispatcher;
import org.fedai.eggroll.core.grpc.URI;
import org.fedai.eggroll.core.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.fedai.eggroll.core.grpc.CommandUri.*;

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
    ClusterManagerService clusterManagerService;
    @Inject
    JobServiceHandler jobServiceHandler;
    @Inject
    RendezvousStoreService rendezvousStoreService;
    @Inject
    ClusterResourceManager clusterResourceManager;


    @Inject
    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.dispatcher.register(this);
    }
    @URI(value= nodeHeartbeat)
    public ErNodeHeartbeat nodeHeartbeat(Context context , ErNodeHeartbeat  erNodeHeartbeat){
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
    public ErStoreList getStoreFromNamespace(Context context , ErStore erStore) {
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
    public ErProcessor heartbeat(Context context , ErProcessor erProcessor) {
        context.setSessionId(erProcessor.getSessionId());
        context.setProcessorId(erProcessor.getId().toString());
        context.putLogData(Dict.STATUS,erProcessor.getStatus());
        return defaultProcessorManager.heartbeat(context, erProcessor);
    }

    @URI(value = stopSession)
    public ErSessionMeta stopSession(Context context ,ErSessionMeta erSessionMeta) {
        context.setSessionId(erSessionMeta.getId());
        context.putData(Dict.STATUS_REASON,StatusReason.API.name());
        logger.info("session will be stoped: sessionId  = {}", erSessionMeta.getId());
        return defaultSessionManager.stopSession(context, erSessionMeta);
    }

    @URI(value = killSession)
    public ErSessionMeta killSession(Context context ,ErSessionMeta erSessionMeta) {
        context.setSessionId(erSessionMeta.getId());
        context.putData(Dict.STATUS_REASON,StatusReason.API.name());
        logger.info("session will be killed: sessionId  = {}", erSessionMeta.getId());
        return defaultSessionManager.killSession(context, erSessionMeta);
    }

    @URI(value = getQueueView)
    public QueueViewResponse getQueueView(Context context,QueueViewRequest queueViewRequest) {
        return defaultSessionManager.getQueueView(context);
    }

    @URI(value = killAllSessions)
    public ErSessionMeta killAllSession(Context context ,ErSessionMeta erSessionMeta) {
        context.setSessionId(erSessionMeta.getId());
        context.putData(Dict.STATUS_REASON,StatusReason.API.name());
        return defaultSessionManager.killAllSessions(context, erSessionMeta);
    }


    @URI(value = submitJob)
    public SubmitJobResponse submitJob(Context context ,SubmitJobRequest request) throws InterruptedException {
        return jobServiceHandler.handleSubmit(request);
    }

    @URI(value = queryJobStatus)
    public QueryJobStatusResponse queryJobStatus(Context context ,QueryJobStatusRequest request){
        return jobServiceHandler.handleJobStatusQuery(request);
    }

    @URI(value = queryJob)
    public QueryJobResponse queryJob(Context context ,QueryJobRequest request){
        return jobServiceHandler.handleJobQuery(request);
    }

    @URI(value = killJob)
    public KillJobResponse killJob(Context context , KillJobRequest request){
        return jobServiceHandler.handleJobKill(context,request);
    }

    @URI(value = stopJob)
    public StopJobResponse stopJob(Context context ,StopJobRequest request){
        return jobServiceHandler.handleJobStop(context,request);
    }


    @URI(value = downloadJob)
    public DownloadJobResponse downloadJob(Context context ,DownloadJobRequest request){
        return jobServiceHandler.handleJobDownload(context,request);
    }

    @URI(value = prepareJobDownload)
    public PrepareJobDownloadResponse prepareJobDownload(Context context ,PrepareJobDownloadRequest request) {
        return jobServiceHandler.prepareJobDownload(context,request);
    }

    @URI(value = rendezvousAdd)
    public RendezvousStoreAddResponse rendezvousAdd(Context context , RendezvousStoreAddRequest request){
        return rendezvousStoreService.add(context,request);
    }

    @URI(value = rendezvousDestroy)
    public RendezvousStoreDestroyResponse rendezvousDestroy(Context context , RendezvousStoreDestroyRequest request) {
        return rendezvousStoreService.destroy(context,request);
    }

    @URI(value = rendezvousSet)
    public RendezvousStoreSetResponse rendezvousSet(Context context , RendezvousStoreSetRequest request){
        return rendezvousStoreService.set(context,request);
    }

    @URI(value = rendezvousGet)
    public RendezvousStoreGetResponse rendezvousGet(Context context , RendezvousStoreGetRequest request) throws InterruptedException {
        return rendezvousStoreService.get(context,request);
    }

    @URI(value = checkResourceEnough)
    public CheckResourceEnoughResponse checkResourceEnough(Context context , CheckResourceEnoughRequest request) throws InterruptedException {
        return clusterResourceManager.checkResourceEnoughForFlow(context, request);
    }

    @URI(value = nodeMetaInfo)
    public MetaInfoResponse queryNodeMetaInfo(Context context, MetaInfoRequest metaInfoRequest) {
        logger.info("=============queryNodeMetaInfo==============");
        MetaInfoResponse metaInfoResponse = new MetaInfoResponse();
        Map<String,String> metaMap = new HashMap<>();
        Map map = MetaInfo.toMap();
        map.keySet().stream().forEach(key -> {
            String strValue = null;
            Object object = map.get(key);
            if (null != object) {
                strValue = String.valueOf(map.get(key));
            }
            metaMap.put(String.valueOf(key),strValue);
        });
        metaInfoResponse.setMetaMap(metaMap);
        return metaInfoResponse;
    }

}
