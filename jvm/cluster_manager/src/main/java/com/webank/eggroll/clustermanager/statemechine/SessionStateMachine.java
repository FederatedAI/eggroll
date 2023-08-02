package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.constant.SessionEvents;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.pojo.ErSessionMeta;

import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class SessionStateMachine extends AbstractStateMachine<ErSessionMeta>{

    @Override
    public String getLockKey(ErSessionMeta erSessionMeta) {
        return erSessionMeta.getId();
    }

    @Override
    public void doChangeStatus(ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        String statusLine = buildStateChangeLine(preStateParam,desStateParam);
        switch ( statusLine){

            //PREPARE(false), NEW(false),NEW_TIMEOUT(true),ACTIVE(false),CLOSED(true),KILLED(true),ERROR(true),FINISHED(true);
            case "_NEW":createSession() ;break;
            case "NEW_ACTIVE" : ;break;
            case "NEW_KILLED" : ;break;
            case "NEW_ERROR" : ;break;
            case "NEW_FINISHED" : ;break;
            case "NEW_CLOSED" : ;break;
            default:
                throw  new RuntimeException();
        }
    }

    @Override
    public ErSessionMeta prepare(ErSessionMeta erSessionMeta) {
        return erSessionMeta;
    }


    private void  createSession(){


    }
}
