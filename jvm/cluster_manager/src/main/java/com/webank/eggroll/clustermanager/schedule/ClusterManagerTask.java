package com.webank.eggroll.clustermanager.schedule;

import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Singleton
public class ClusterManagerTask {

    public static void runTask(Thread thread) {
        thread.start();
    }

}
