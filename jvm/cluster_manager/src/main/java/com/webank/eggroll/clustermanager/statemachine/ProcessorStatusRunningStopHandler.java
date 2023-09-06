package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;



@Singleton
public class ProcessorStatusRunningStopHandler  extends  AbstractProcessorStateHandler {

    @Inject
    ResourceStateMechine  resourceStateMachine ;

    @Inject
    ProcessorService processorService;

    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErProcessor handle(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        String  line =   preStateParam+"_"+desStateParam;
//        switch (line){
//            case "NEW_RUNNING":
//                updateState(data,desStateParam);
//                if(this.checkNeedChangeResource(data)) {
//                    resourceStateMachine.changeStatus(context, data, ResourceStatus.PRE_ALLOCATED.getValue(), ResourceStatus.ALLOCATED.getValue());
//                }
//
//
//                break;
//            case "NEW_STOPPED":
//            case "NEW_KILLED":
//            case "NEW_ERROR":
//                updateState(data,desStateParam);
//                if(this.checkNeedChangeResource(data)) {
//                    resourceStateMachine.changeStatus(context, data,  ResourceStatus.PRE_ALLOCATED.getValue(),ResourceStatus.ALLOCATE_FAILED.getValue());
//                }
//                break;
//        }
        updateState(data,desStateParam);
        if(this.checkNeedChangeResource(data)) {
                    resourceStateMachine.changeStatus(context, data, ResourceStatus.PRE_ALLOCATED.getValue(), ResourceStatus.ALLOCATED.getValue());
                }

        SessionProcessor  result =  this.processorService.getById(data.getId());
        if(result==null){
            throw  new RuntimeException("");
        }
        return result.toErProcessor();
    }

//    void   updateState(ErProcessor  erProcessor,String desStateParam){
//        SessionProcessor  sessionProcessor =  new SessionProcessor();
//        sessionProcessor.setStatus(desStateParam);
//        sessionProcessor.setProcessorId(erProcessor.getId());
//        this.processorService.updateById(sessionProcessor);
//    }


}
