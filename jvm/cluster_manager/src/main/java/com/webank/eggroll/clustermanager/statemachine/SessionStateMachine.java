package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import org.apache.commons.lang3.StringUtils;



@Singleton
public class SessionStateMachine extends AbstractStateMachine<ErSessionMeta>   {

    @Inject
    SessionMainService   sessionMainService;

    @Inject
    ProcessorStateMachine processorStateMachine;

    @Inject
    SessionKillHandler  sessionKillHandler;

    @Inject
    SessionStopHandler  sessionStopHandler;

    @Inject
    SessionActiveHandler  sessionActiveHandler;

    @Inject
    SessionCreateHandler  sessionCreateHandler;

    @Override
    String buildStateChangeLine(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        String  line= "";
        ErSessionMeta  sessionInDb = sessionMainService.getSession(erSessionMeta.getId(),true,false,false);
        if(sessionInDb!=null){
            context.putData(Dict.SESSION_IN_DB,sessionInDb);
        }
        if(StringUtils.isEmpty(preStateParam))
        {
            if(sessionInDb==null){
                preStateParam ="";
            }else{
                preStateParam =  sessionInDb.getStatus();
            }
        }
        line= preStateParam+"_"+desStateParam;
        context.putLogData("session_status_change",line);
        return  line;
    }

    @Override
    public String getLockKey(ErSessionMeta erSessionMeta) {
        return erSessionMeta.getId();
    }

    @Inject
    public void afterPropertiesSet() throws Exception {
        this.registeStateHander("_NEW",sessionCreateHandler);
        this.registeStateHander("NEW_ACTIVE",sessionActiveHandler);
        this.registeStateHander("NEW_KILLED",sessionKillHandler);
        this.registeStateHander("NEW_ERROR",sessionKillHandler);
        this.registeStateHander("ACTIVE_KILLED",sessionKillHandler);
        this.registeStateHander("ACTIVE_ERROR",sessionKillHandler);
        this.registeStateHander("ACTIVE_CLOSED",sessionStopHandler);
    }

}
