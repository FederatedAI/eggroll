package com.eggroll.core.grpc;


import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;

import static com.eggroll.core.grpc.CommandUri.heartbeat;
import static com.eggroll.core.grpc.CommandUri.startContainers;

public class ClusterManagerClient {

    CommandClient  cc;
    ErEndpoint endpoint;
    public  ClusterManagerClient(ErEndpoint endpoint){
        if (endpoint == null )
            throw new IllegalArgumentException("failed to create NodeManagerClient for endpoint: " + endpoint);
        this.endpoint = endpoint;
        cc = new CommandClient();
    }

    public ErProcessor hearbeat(ErProcessor processor) {
        byte[] responseData = cc.call(endpoint, heartbeat,processor.serialize());
        ErProcessor response = new ErProcessor();
        response.deserialize(responseData);

        return response;

    }

}
