package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.SessionProcessorService;
import org.fedai.eggroll.clustermanager.entity.SessionProcessor;
import org.fedai.eggroll.core.constant.ResourceStatus;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;


@Singleton
public class ProcessorCreateHandler extends AbstractProcessorStateHandler {


    @Inject
    SessionProcessorService sessionProcessorService;


    @Inject
    ResourceStateMechine resourceStateMechine;


    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return createNewProcessor(context, data);
    }

    private ErProcessor createNewProcessor(Context context, ErProcessor erProcessor) {
        erProcessor.setId(erProcessor.getId() == -1 ? null : erProcessor.getId());
        SessionProcessor sessionProcessor = new SessionProcessor(erProcessor);
        sessionProcessorService.save(sessionProcessor);
        erProcessor.setId(sessionProcessor.getProcessorId());
        if (checkNeedChangeResource(erProcessor)) {
            resourceStateMechine.changeStatus(context, erProcessor, ResourceStatus.INIT.getValue(), ResourceStatus.PRE_ALLOCATED.getValue());
        }
        return erProcessor;
    }


}
