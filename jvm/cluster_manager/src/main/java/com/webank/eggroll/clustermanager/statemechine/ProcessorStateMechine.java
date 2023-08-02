package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.constant.ResourceStatus;
import com.eggroll.core.pojo.ErProcessor;

import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
@Service
public class ProcessorStateMechine extends  AbstractStateMachine<ErProcessor>{
    Logger logger = LoggerFactory.getLogger(ProcessorStateMechine.class);
    @Autowired
    ProcessorService  processorService;
    @Autowired
    ResourceStateMechine  resourceStateMechine;

    @Override
    public String getLockKey(ErProcessor erProcessor) {
        return Long.toString(erProcessor.getId());
    }
    private  boolean  checkNeedChangeResource(ErProcessor erProcessor){

//        if( dispatchConfig=="true"||processorType=="DeepSpeed") {
//            return true;
//        }
        return  true;
    }



    @Transactional
    private  ErProcessor   createNewProcessor(ErProcessor erProcessor){
        processorService.save(new SessionProcessor(erProcessor));
        if(checkNeedChangeResource(erProcessor)) {
            resourceStateMechine.changeStatus(erProcessor, ResourceStatus.INIT.getValue(), ResourceStatus.PRE_ALLOCATED.getValue());
        }
        return  erProcessor;
    }

    @Transactional
    private void  updateProcessorState(ErProcessor erProcessor ,Callback<ErProcessor>  callback ){
        SessionProcessor  sessionProcessor = new SessionProcessor(erProcessor);
        processorService.updateById(sessionProcessor);
        if(callback!=null){
            callback.callback(erProcessor);
        }

    }



    @Override
    public void doChangeStatus(ErProcessor erProcessor, String preStateParam, String desStateParam) {
        String statusLine = buildStateChangeLine(preStateParam,desStateParam);
        erProcessor.setStatus(desStateParam);

        switch ( statusLine){

            //PREPARE(false), NEW(false),NEW_TIMEOUT(true),ACTIVE(false),CLOSED(true),KILLED(true),ERROR(true),FINISHED(true);
            case "_NEW":  createNewProcessor(erProcessor);break;
            case "NEW_RUNNING" :

                updateProcessorState(erProcessor,(ep)->{
                    if(checkNeedChangeResource(ep)){
                        resourceStateMechine.changeStatus(ep, ResourceStatus.PRE_ALLOCATED.getValue(), ResourceStatus.ALLOCATED.getValue());
                    }
                });break;
            case "NEW_STOPPED" : ;
            case "NEW_KILLED" : ;
            case "NEW_ERROR" :
                updateProcessorState(erProcessor,(ep)->{
                    if(checkNeedChangeResource(ep)){
                        resourceStateMechine.changeStatus(ep, ResourceStatus.ALLOCATED.getValue(), ResourceStatus.ALLOCATE_FAILED.getValue());
                    }
                });break;

            case "RUNNING_FINISHED" : ;
            case "RUNNING_STOPPED" : ;
            case "RUNNING_KILLED" : ;
            case "RUNNING_ERROR" :
                updateProcessorState(erProcessor,(ep)->{
                    if(checkNeedChangeResource(ep)){
                        resourceStateMechine.changeStatus(ep, ResourceStatus.ALLOCATED.getValue(), ResourceStatus.RETURN.getValue());
                    }
                });break;

            default:

        }

    }

    @Override
    public ErProcessor prepare(ErProcessor erProcessor) {

        if(StringUtils.isEmpty(erProcessor.getStatus())){
            SessionProcessor sessionProcessor = processorService.getById(erProcessor.getId());
            if(sessionProcessor!=null)
                return sessionProcessor.toErProcessor();
            else
                throw new RuntimeException("");
        }else{
            return  erProcessor;
        }

    }
}



