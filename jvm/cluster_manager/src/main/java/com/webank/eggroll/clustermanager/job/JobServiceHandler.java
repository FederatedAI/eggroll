package com.webank.eggroll.clustermanager.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class JobServiceHandler {
    Logger log = LoggerFactory.getLogger(JobServiceHandler.class);

    public void killJob(String sessionId,Boolean isTimeout){
        log.info("killing job {}",sessionId);

        try {

        }catch (Exception e ){
            log.
        }
    }
}
