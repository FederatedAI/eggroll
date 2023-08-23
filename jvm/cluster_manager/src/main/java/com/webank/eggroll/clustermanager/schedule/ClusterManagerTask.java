package com.webank.eggroll.clustermanager.schedule;

import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClusterManagerTask {

    public static void runTask(Thread thread) {
        thread.start();
    }

}
