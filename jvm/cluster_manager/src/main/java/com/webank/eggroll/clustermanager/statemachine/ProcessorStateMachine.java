package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Singleton
public class ProcessorStateMachine extends  AbstractStateMachine<ErProcessor>  implements InitializingBean {
    Logger logger = LoggerFactory.getLogger(ProcessorStateMachine.class);
    @Autowired
    @Inject
    ProcessorService  processorService;
    @Autowired
    @Inject
    ResourceStateMechine  resourceStateMechine;
    @Autowired
    @Inject
    ProcessorStateRunningHandler     processorStateRunningHandler;
    @Autowired
    @Inject
    ProcessorStatusRunningStopHandler       processorStatusRunningStopHandler  ;
    @Autowired
    @Inject
    ProcessorStateNewStopHandler   processorStateNewStopHandler;

    @Autowired
    @Inject
    ProcessorCreateHandler   processorCreateHandler;


//    public List<ErProcessor>  getProcessorBySessionId(String sessionId){
//        return processorService.getProcessorBySession(sessionId);
//    }


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



    @Override
    public void afterPropertiesSet() throws Exception {
        this.registeStateHander( "_NEW",processorCreateHandler);
        this.registeStateHander( "NEW_RUNNING",processorCreateHandler);
        this.registeStateHander("RUNNING_FINISHED",processorStatusRunningStopHandler);
        this.registeStateHander("RUNNING_STOPPED",processorStatusRunningStopHandler);
        this.registeStateHander("RUNNING_KILLED",processorStatusRunningStopHandler);
        this.registeStateHander("RUNNING_ERROR",processorStatusRunningStopHandler);
        this.registeStateHander("NEW_FINISHED",processorStateNewStopHandler);
        this.registeStateHander("NEW_STOPPED",processorStateNewStopHandler);
        this.registeStateHander("NEW_KILLED",processorStateNewStopHandler);
        this.registeStateHander("NEW_ERROR",processorStateNewStopHandler);

    }

}



