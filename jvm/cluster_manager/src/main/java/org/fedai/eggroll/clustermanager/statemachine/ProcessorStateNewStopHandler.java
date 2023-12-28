package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.core.constant.ResourceStatus;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;


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
