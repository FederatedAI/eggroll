package com.webank.eggroll.clustermanager.job;


import com.eggroll.core.grpc.CommandClient;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
import com.eggroll.core.utils.NetUtils;
import com.google.inject.Inject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

public class TestJobServiceHandler {

    ErEndpoint endpoint = new ErEndpoint(NetUtils.getLocalHost(),4670);

    @Inject
    JobServiceHandler jobServiceHandler;

    public byte[] sendGrpc(ErEndpoint endpoint , String uri ,RpcMessage message){
        byte[] response = new CommandClient().call(endpoint, uri, message.serialize());
        return response;
    }

    @Test
    public void testSubmitJob() throws InterruptedException {
        SubmitJobRequest request = new SubmitJobRequest();
        request.setJobType(JobProcessorTypes.DeepSpeed.name());
        request.setSessionId("testSubmit_"+ System.currentTimeMillis());
        request.setWorldSize(10);
        request.setName("TestSubmit");
        request.setCommandArguments(new ArrayList<>());
        request.setEnvironmentVariables(new HashMap<>());
        request.setFiles(new HashMap<>());
        request.setZippedFiles(new HashMap<>());

        ResourceOptions resourceOptions = new ResourceOptions();
        resourceOptions.setTimeoutSeconds(60);
        request.setResourceOptions(resourceOptions);

        request.setOptions(new HashMap<>());
//        sendGrpc(endpoint,)
//        SubmitJobResponse submitJobResponse = jobServiceHandler.handleSubmit(request);
//        System.err.println("=============== => " + JsonUtil.object2Json(submitJobResponse));
    }
}
