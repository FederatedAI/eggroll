package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Date;

public abstract class AbstractSessionStateHandler implements   StateHandler<ErSessionMeta>{

    Logger logger = LoggerFactory.getLogger(AbstractSessionStateHandler.class);

    @Inject
    SessionMainService  sessionMainService;
    @Inject
    ProcessorStateMachine processorStateMachine;
    @Inject
    ServerNodeService   serverNodeService;

    void  updateStatus(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam){
        SessionMain sessionMain =  new SessionMain();
        sessionMain.setSessionId(erSessionMeta.getId());
        sessionMain.setStatus(desStateParam);
        sessionMain.setActiveProcCount(erSessionMeta.getActiveProcCount());
        sessionMainService.updateById(sessionMain);
    }

    void  doInserSession(Context context ,ErSessionMeta erSessionMeta){
        SessionMain  sessionMain = new  SessionMain(erSessionMeta.getId(),erSessionMeta.getName(), SessionStatus.NEW.name(),
                erSessionMeta.getTag(),erSessionMeta.getTotalProcCount(),erSessionMeta.getActiveProcCount(),new Date(),new Date());
        sessionMainService.save(sessionMain);
        erSessionMeta.getProcessors().forEach(p->{
            logger.info("prepare to handle processor {}",p);
            processorStateMachine.changeStatus(context,p,null, SessionStatus.NEW.name());
        });

    }



}
