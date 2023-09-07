package com.eggroll.core.grpc;


import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.*;

import static com.eggroll.core.grpc.CommandUri.*;

public class ClusterManagerClient {

    CommandClient cc;
    ErEndpoint endpoint;

    public ClusterManagerClient(ErEndpoint endpoint) {
        if (endpoint == null)
            throw new IllegalArgumentException("failed to create NodeManagerClient for endpoint: " + endpoint);
        this.endpoint = endpoint;
        cc = new CommandClient();
    }


    public ErProcessor hearbeat(Context context , ErProcessor processor) {
        byte[] responseData = cc.call(context,endpoint, heartbeat, processor.serialize());
        ErProcessor response = new ErProcessor();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta getOrCreateSession(Context context,ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context,endpoint, getOrCreateSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta getSession(Context context,ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context ,endpoint, getSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta killSession(Context context ,ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context,endpoint, killSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta killAllSession(Context context,ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(context ,endpoint, killAllSessions, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErStore   getOrCreateStore(Context context,ErStore erStore){
        byte[] responseData = cc.call(context ,endpoint, getOrCreateStore, erStore.serialize());
        ErStore response = new ErStore();
        response.deserialize(responseData);
        return response;
    }


//    def nodeHeartbeat(node: ErNodeHeartbeat): ErNodeHeartbeat =
//    cc.call[ErNodeHeartbeat](ManagerCommands.nodeHeartbeat, node)

    public ErNodeHeartbeat nodeHeartbeat(Context context ,ErNodeHeartbeat node) {
        byte[] responseData = cc.call(context,endpoint, nodeHeartbeat, node.serialize());
        ErNodeHeartbeat response = new ErNodeHeartbeat();
        response.deserialize(responseData);
        return response;
    }

    public PrepareJobDownloadResponse   prepareJobDownload(Context context,PrepareJobDownloadRequest  prepareJobDownloadRequest){
        byte[] responseData = cc.call(context ,endpoint, prepareJobDownload, prepareJobDownloadRequest.serialize());
        PrepareJobDownloadResponse response = new PrepareJobDownloadResponse();
        response.deserialize(responseData);
        return response;
    }

}