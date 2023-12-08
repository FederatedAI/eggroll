package org.fedai.eggroll.clustermanager.schedule;

import com.google.inject.Inject;
import com.google.inject.Singleton;


@Singleton
public class ClusterManagerTask {

    @Inject
    Quartz quartz;


    public static void runTask(Thread thread) {
        thread.start();
    }



}
