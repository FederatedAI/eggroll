package com.eggroll.core.grpc;


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


    public ErProcessor hearbeat(ErProcessor processor) {
        byte[] responseData = cc.call(endpoint, heartbeat, processor.serialize());
        ErProcessor response = new ErProcessor();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta getOrCreateSession(ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(endpoint, getOrCreateSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta getSession(ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(endpoint, getSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta killSession(ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(endpoint, killSession, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta killAllSession(ErSessionMeta erSessionMeta) {
        byte[] responseData = cc.call(endpoint, killAllSessions, erSessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }


//    def nodeHeartbeat(node: ErNodeHeartbeat): ErNodeHeartbeat =
//    cc.call[ErNodeHeartbeat](ManagerCommands.nodeHeartbeat, node)

    public ErNodeHeartbeat nodeHeartbeat(ErNodeHeartbeat node) {
        byte[] responseData = cc.call(endpoint, nodeHeartbeat, node.serialize());
        ErNodeHeartbeat response = new ErNodeHeartbeat();
        response.deserialize(responseData);
        return response;
    }

    public PrepareJobDownloadResponse   prepareJobDownload(PrepareJobDownloadRequest  prepareJobDownloadRequest){
        byte[] responseData = cc.call(endpoint, prepareJobDownload, prepareJobDownloadRequest.serialize());
        PrepareJobDownloadResponse response = new PrepareJobDownloadResponse();
        response.deserialize(responseData);
        return response;
    }

}
