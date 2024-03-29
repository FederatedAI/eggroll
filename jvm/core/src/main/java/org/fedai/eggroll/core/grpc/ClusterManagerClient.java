package org.fedai.eggroll.core.grpc;


import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManagerClient {

    CommandClient cc;
    ErEndpoint endpoint;
    Logger logger = LoggerFactory.getLogger(ClusterManagerClient.class);
    public ClusterManagerClient(ErEndpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("failed to create NodeManagerClient for endpoint: " + endpoint);
        }
        this.endpoint = endpoint;
        cc = new CommandClient();
    }


    public ErProcessor hearbeat(Context context, ErProcessor processor) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.heartbeat, processor.serialize());
        ErProcessor response = new ErProcessor();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta getOrCreateSession(Context context, ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.getOrCreateSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta getSession(Context context, ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.getSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta killSession(Context context, ErSessionMeta erSessionMeta) {
        logger.info("============> send killSession command to nodeManager  sessionId = {}",erSessionMeta.getId());
        byte[] responseData = cc.call(context, endpoint, CommandUri.killSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    // 获取队列中的任务数量
    public QueueViewResponse getQueueView(Context context,QueueViewRequest queueViewRequest) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.getQueueView, queueViewRequest.serialize());
        QueueViewResponse response = new QueueViewResponse();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta killAllSession(Context context, ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.killAllSessions, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErStore getOrCreateStore(Context context, ErStore erStore) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.getOrCreateStore, erStore.serialize());
        ErStore response = new ErStore();
        response.deserialize(responseData);
        return response;
    }


//    def nodeHeartbeat(node: ErNodeHeartbeat): ErNodeHeartbeat =
//    cc.call[ErNodeHeartbeat](ManagerCommands.nodeHeartbeat, node)

    public ErNodeHeartbeat nodeHeartbeat(Context context, ErNodeHeartbeat node) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.nodeHeartbeat, node.serialize());
        ErNodeHeartbeat response = new ErNodeHeartbeat();
        response.deserialize(responseData);
        return response;
    }

    public PrepareJobDownloadResponse prepareJobDownload(Context context, PrepareJobDownloadRequest prepareJobDownloadRequest) {
        byte[] responseData = cc.call(context, endpoint, CommandUri.prepareJobDownload, prepareJobDownloadRequest.serialize());
        PrepareJobDownloadResponse response = new PrepareJobDownloadResponse();
        response.deserialize(responseData);
        return response;
    }

    public KillJobResponse killJob(Context context , KillJobRequest request){
        byte[] responseData = cc.call(context, endpoint, CommandUri.killJob, request.serialize());
        KillJobResponse response = new KillJobResponse();
        response.deserialize(responseData);
        return response;
    }

    public CheckResourceEnoughResponse checkResourceEnough(Context context , CheckResourceEnoughRequest request){
        byte[] responseData = cc.call(context, endpoint, CommandUri.checkResourceEnough, request.serialize());
        CheckResourceEnoughResponse response = new CheckResourceEnoughResponse();
        response.deserialize(responseData);
        return response;
    }

}
