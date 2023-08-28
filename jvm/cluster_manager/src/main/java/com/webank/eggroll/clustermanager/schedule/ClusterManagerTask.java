package com.webank.eggroll.clustermanager.schedule;

import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;

@Singleton
public class ClusterManagerTask {

    public static void runTask(Thread thread) {
        thread.start();
    }

}
