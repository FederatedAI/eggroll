package com.webank.eggroll.clustermanager.job;

import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class JobServiceHandler {
    Logger log = LoggerFactory.getLogger(JobServiceHandler.class);

    @Autowired
    ClusterResourceManager clusterResourceManager;

    public void killJob(String sessionId,Boolean isTimeout){
        log.info("killing job {}",sessionId);

        try {
            clusterResourceManager.lockSession();
        }catch (Exception e ){

        }
    }
}
