package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.SessionProcessorService;
import org.fedai.eggroll.clustermanager.entity.SessionProcessor;
import org.fedai.eggroll.core.constant.ResourceStatus;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;


@Singleton
public class ProcessorStatusRunningStopHandler extends AbstractProcessorStateHandler {

    @Inject
    ResourceStateMechine resourceStateMachine;

    @Inject
    SessionProcessorService sessionProcessorService;

    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        String line = preStateParam + "_" + desStateParam;
        updateState(data, desStateParam);
        if (this.checkNeedChangeResource(data)) {
            resourceStateMachine.changeStatus(context, data, ResourceStatus.ALLOCATED.getValue(), ResourceStatus.RETURN.getValue());
        }

        SessionProcessor result = sessionProcessorService.getById(data.getId());
        if (result == null) {
            throw new RuntimeException("");
        }
        return result.toErProcessor();
    }


}
