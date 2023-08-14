package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractSessionStateHandler implements   StateHandler<ErSessionMeta>{
    @Autowired
    SessionMainService  sessionMainService;
    @Autowired
    ProcessorStateMachine processorStateMachine;
    @Autowired
    ServerNodeService   serverNodeService;



    void  updateStatus(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam){
        SessionMain sessionMain =  new SessionMain();
        sessionMain.setSessionId(erSessionMeta.getId());
        sessionMain.setStatus(desStateParam);
        sessionMain.setActiveProcCount(erSessionMeta.getActiveProcCount());
        sessionMainService.updateById(sessionMain);
    }

    void  doInserSession(Context context ,ErSessionMeta erSessionMeta){
        SessionMain  sessionMain = new  SessionMain(erSessionMeta.getId(),erSessionMeta.getName(),erSessionMeta.getStatus(),
                erSessionMeta.getTag(),erSessionMeta.getTotalProcCount(),erSessionMeta.getActiveProcCount(),null,null);
        sessionMainService.save(sessionMain);
        erSessionMeta.getProcessors().forEach(p->{
            processorStateMachine.changeStatus(context,p,null, ProcessorStatus.NEW.name());
        });

    }



}
