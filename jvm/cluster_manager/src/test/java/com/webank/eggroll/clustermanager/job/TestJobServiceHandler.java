package com.webank.eggroll.clustermanager.job;


import com.eggroll.core.pojo.JobProcessorTypes;
import com.eggroll.core.pojo.ResourceOptions;
import com.eggroll.core.pojo.SubmitJobRequest;
import com.eggroll.core.pojo.SubmitJobResponse;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.clustermanager.Application;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;

@SpringBootTest(classes = Application.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class TestJobServiceHandler {

    @Autowired
    JobServiceHandler jobServiceHandler;

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
        resourceOptions.setTimeoutSeconds(5);
        request.setResourceOptions(resourceOptions);

        request.setOptions(new HashMap<>());
        SubmitJobResponse submitJobResponse = jobServiceHandler.handleSubmit(request);
        System.err.println("=============== => " + JsonUtil.object2Json(submitJobResponse));
    }
}
