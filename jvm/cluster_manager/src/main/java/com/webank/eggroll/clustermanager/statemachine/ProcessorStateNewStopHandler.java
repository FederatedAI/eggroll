package com.webank.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;


import java.util.Date;


@Singleton
public class ProcessorStateNewStopHandler extends AbstractProcessorStateHandler {

    @Inject
    ResourceStateMechine resourceStateMachine;

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        this.updateState(data, desStateParam);
        if (this.checkNeedChangeResource(data)) {
            resourceStateMachine.changeStatus(context, data, ResourceStatus.PRE_ALLOCATED.getValue(), ResourceStatus.ALLOCATE_FAILED.getValue());
        }
        return data;
    }
}
