package com.webank.eggroll.clustermanager.job;


import com.eggroll.core.grpc.CommandClient;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.NetUtils;
import com.google.inject.Inject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static com.eggroll.core.grpc.CommandUri.*;

public class TestJobServiceHandler {

    ErEndpoint endpoint = new ErEndpoint(NetUtils.getLocalHost(),4670);

    @Inject
    JobServiceHandler jobServiceHandler;

    public byte[] sendGrpc(ErEndpoint endpoint , String uri ,RpcMessage message){
        return new CommandClient().call(endpoint, uri, message.serialize());
    }

    @Test
    public void testSubmitJob() throws InterruptedException {
        SubmitJobRequest request = new SubmitJobRequest();
        request.setJobType(JobProcessorTypes.DeepSpeed.name());
        request.setSessionId("testx_1693463937323");
        request.setWorldSize(2);
        request.setName("TestSubmit");
        request.setCommandArguments(new ArrayList<>());
        request.setEnvironmentVariables(new HashMap<>());
        request.setFiles(new HashMap<>());
        request.setZippedFiles(new HashMap<>());

        ResourceOptions resourceOptions = new ResourceOptions();
        resourceOptions.setTimeoutSeconds(60);
        request.setResourceOptions(resourceOptions);

        request.setOptions(new HashMap<>());
        byte[] bytes = sendGrpc(endpoint, submitJob, request);
        SubmitJobResponse response = new SubmitJobResponse();
        response.deserialize(bytes);
        System.out.println(response);
    }

    @Test
    public void testQueryJobStatus() throws InterruptedException {
        QueryJobStatusRequest request = new QueryJobStatusRequest();
        request.setSessionId("testx_1693463937323");
        byte[] bytes = sendGrpc(endpoint, queryJobStatus, request);
        QueryJobStatusResponse response = new QueryJobStatusResponse();
        response.deserialize(bytes);
        System.out.println(response);
    }

    @Test
    public void testQueryJob() throws InterruptedException {
        QueryJobRequest request = new QueryJobRequest();
        request.setSessionId("testx_1693463937323");
        byte[] bytes = sendGrpc(endpoint, queryJob, request);
        QueryJobResponse response = new QueryJobResponse();
        response.deserialize(bytes);
        System.out.println(response);
    }

    @Test
    public void testStopJob() throws InterruptedException {
        StopJobRequest request = new StopJobRequest();
        request.setSessionId("testx_1693463937323");
        byte[] bytes = sendGrpc(endpoint, queryJob, request);
        StopJobResponse response = new StopJobResponse();
        response.deserialize(bytes);
        System.out.println(response);
    }


    @Test
    public void testKillJob() throws InterruptedException {
        KillJobRequest request = new KillJobRequest();
        request.setSessionId("testx_1693536983025");
        byte[] bytes = sendGrpc(endpoint, queryJob, request);
        KillJobRequest response = new KillJobRequest();
        response.deserialize(bytes);
        System.out.println(response);
    }
}
