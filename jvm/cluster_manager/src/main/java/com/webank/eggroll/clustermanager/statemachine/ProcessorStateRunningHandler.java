package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class ProcessorStateRunningHandler   extends  AbstractProcessorStateHandler {
    Logger logger = LoggerFactory.getLogger(ProcessorStateRunningHandler.class);

    @Inject
    SessionMainService  sessionMainService;
    @Inject
    ProcessorService  processorService;

    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        ErProcessor  erProcessor  = (ErProcessor) context.getData(Dict.PROCESSOR_IN_DB);
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        this.updateState(data,desStateParam);
        if(this.checkNeedChangeResource(data)){
            resourceStateMechine.changeStatus(context,data, ResourceStatus.PRE_ALLOCATED.getValue(),ResourceStatus.ALLOCATE_FAILED.getValue());
        }
        this.openAsynPostHandle(context);
        return processorService.getById(data.getId()).toErProcessor();
    }


    public void asynPostHandle(Context context, ErProcessor data, String preStateParam, String desStateParam){
        logger.info("prepare to update session main active count {}",data.getSessionId());
        sessionMainService. updateSessionMainActiveCount(data.getSessionId());
    };


}
