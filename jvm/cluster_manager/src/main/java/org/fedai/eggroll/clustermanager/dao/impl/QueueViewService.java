package org.fedai.eggroll.clustermanager.dao.impl;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.cluster.ClusterResourceManager;
import org.fedai.eggroll.core.pojo.FifoBroker;
import org.fedai.eggroll.core.pojo.QueueViewResponse;
import org.fedai.eggroll.core.pojo.ResourceApplication;

import java.util.ArrayList;

@Singleton
public class QueueViewService {

    private final FifoBroker<ResourceApplication> applicationQueue;

    private ClusterResourceManager clusterResourceManager;

    @Inject
    public QueueViewService(ClusterResourceManager clusterResourceManager) {
        this.applicationQueue = clusterResourceManager.getApplicationQueue();
    }

    public QueueViewResponse viewQueue() {
        ArrayList<ResourceApplication> list = new ArrayList<>(applicationQueue.getBroker());
        QueueViewResponse queueViewResponse = new QueueViewResponse();
        if (list.size() > 0 && !list.isEmpty()) {
            queueViewResponse.setQueueSize(list.size());
            return queueViewResponse;
        }
        queueViewResponse.setQueueSize(0);
        return queueViewResponse;
    }
}
