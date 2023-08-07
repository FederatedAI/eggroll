package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessorStateMechine extends  AbstractStateMachine<ErProcessor>  implements InitializingBean {
    Logger logger = LoggerFactory.getLogger(ProcessorStateMechine.class);
    @Autowired
    ProcessorService  processorService;
    @Autowired
    ResourceStateMechine  resourceStateMechine;
    @Autowired
    ProcessorStateRunningHandler     processorStateRunningHandler;
    @Autowired
    ProcessorStatusNewHandler     processorStatusNewHandler;


    @Override
    String buildStateChangeLine(Context context, ErProcessor erProcessor, String preStateParam, String desStateParam) {
        String  line= "";
        SessionProcessor processorInDb = processorService.getById(erProcessor.getId());
        if(processorInDb!=null){
            context.putData(Dict.PROCESSOR_IN_DB,processorInDb.toErProcessor());
        }
        if(StringUtils.isEmpty(preStateParam))
        {
            if(processorInDb==null){
                preStateParam ="";
            }else{
                preStateParam =  processorInDb.getStatus();
            }
        }
        line= preStateParam+"_"+desStateParam;
        return  line;
    }

    @Override
    public String getLockKey(ErProcessor erProcessor) {
        return Long.toString(erProcessor.getId());
    }







//    private  ErProcessor   createNewProcessor(Context context ,ErProcessor erProcessor){
//        processorService.save(new SessionProcessor(erProcessor));
//        if(checkNeedChangeResource(erProcessor)) {
//            resourceStateMechine.changeStatus(context ,erProcessor, ResourceStatus.INIT.getValue(), ResourceStatus.PRE_ALLOCATED.getValue());
//        }
//        return  erProcessor;
//
//    }


//    private void  updateProcessorState(ErProcessor erProcessor ,Callback<ErProcessor>  callback ){
//        SessionProcessor  sessionProcessor = new SessionProcessor(erProcessor);
//        processorService.updateById(sessionProcessor);
//        if(callback!=null){
//            callback.callback(erProcessor);
//        }
//
//    }



//    @Override
//    protected ErProcessor doChangeStatus(Context context , ErProcessor erProcessor, String preStateParam, String desStateParam) {
//        String statusLine = buildStateChangeLine(preStateParam,desStateParam);
//        erProcessor.setStatus(desStateParam);
//
//        switch ( statusLine){
//
//            //PREPARE(false), NEW(false),NEW_TIMEOUT(true),ACTIVE(false),CLOSED(true),KILLED(true),ERROR(true),FINISHED(true);
//            case "_NEW":  createNewProcessor(context,erProcessor);break;
//            case "NEW_RUNNING" :
//
//                updateProcessorState(erProcessor,(ep)->{
//                    if(checkNeedChangeResource(ep)){
//                        resourceStateMechine.changeStatus(context,ep, ResourceStatus.PRE_ALLOCATED.getValue(), ResourceStatus.ALLOCATED.getValue());
//                    }
//                });break;
//            case "NEW_STOPPED" : ;
//            case "NEW_KILLED" : ;
//            case "NEW_ERROR" :
//                updateProcessorState(erProcessor,(ep)->{
//                    if(checkNeedChangeResource(ep)){
//                        resourceStateMechine.changeStatus(context ,ep, ResourceStatus.ALLOCATED.getValue(), ResourceStatus.ALLOCATE_FAILED.getValue());
//                    }
//                });break;
//
//            case "RUNNING_FINISHED" : ;
//            case "RUNNING_STOPPED" : ;
//            case "RUNNING_KILLED" : ;
//            case "RUNNING_ERROR" :
//                updateProcessorState(erProcessor,(ep)->{
//                    if(checkNeedChangeResource(ep)){
//                        resourceStateMechine.changeStatus(context,ep, ResourceStatus.ALLOCATED.getValue(), ResourceStatus.RETURN.getValue());
//                    }
//                });break;
//
//            default:
//
//        }
//        return null;
//
//    }

//    @Override
//    public ErProcessor prepare(Context  context ,ErProcessor erProcessor) {
//
//        if(StringUtils.isEmpty(erProcessor.getStatus())){
//            SessionProcessor sessionProcessor = processorService.getById(erProcessor.getId());
//            if(sessionProcessor!=null)
//                return sessionProcessor.toErProcessor();
//            else
//                throw new RuntimeException("");
//        }else{
//            return  erProcessor;
//        }
//
//    }

    @Override
    public void afterPropertiesSet() throws Exception {
      //  this.registeStateHander("_NEW",new);
    }
}



