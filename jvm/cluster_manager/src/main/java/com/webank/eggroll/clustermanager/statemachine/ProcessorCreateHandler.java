package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProcessorCreateHandler extends  AbstractProcessorStateHandler {

    @Autowired
    ProcessorService processorService;

    @Autowired
    ResourceStateMechine  resourceStateMechine;


    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return  createNewProcessor(context,data);
    }

    private  ErProcessor createNewProcessor(Context context ,ErProcessor erProcessor){
        erProcessor.setId(erProcessor.getId() == -1 ? null : erProcessor.getId());
        SessionProcessor sessionProcessor = new SessionProcessor(erProcessor);
        processorService.save(sessionProcessor);
        erProcessor.setId(sessionProcessor.getProcessorId());
        if(checkNeedChangeResource(erProcessor)) {
            resourceStateMechine.changeStatus(context ,erProcessor, ResourceStatus.INIT.getValue(), ResourceStatus.PRE_ALLOCATED.getValue());
        }
        return  erProcessor;
    }


}
