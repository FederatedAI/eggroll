package com.webank.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;

@Singleton
public class ProcessorStateInvalidHandler extends AbstractProcessorStateHandler {
    @Inject
    ClusterManagerService clusterManagerService;

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        clusterManagerService.addResidualHeartbeat(data);
        return data;
    }

}
