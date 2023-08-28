package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Singleton;



@Singleton
public class ProcessorStateRunningHandler   extends  AbstractProcessorStateHandler {

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
        return processorService.getById(data.getId()).toErProcessor();
    }

}
