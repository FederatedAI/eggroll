package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import com.webank.eggroll.clustermanager.processor.DefaultProcessorManager;

@Singleton
public class ProcessorStateInvalidHandler extends AbstractProcessorStateHandler{
    @Inject
    ClusterManagerService clusterManagerService;

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        clusterManagerService.addResidualHeartbeat(data);
        return data;
    }

}
