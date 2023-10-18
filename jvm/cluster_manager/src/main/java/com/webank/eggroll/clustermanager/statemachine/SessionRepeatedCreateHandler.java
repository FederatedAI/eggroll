package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.exceptions.ErSessionException;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.session.DefaultSessionManager;

@Singleton
public class SessionRepeatedCreateHandler extends  AbstractSessionStateHandler{
    @Inject
    SessionMainService  sessionMainService;
    @Inject
    DefaultSessionManager sessionManager;
    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
       if(!checkSessionRpcReady(data)) {
           ErSessionMeta erSessionMeta = this.sessionMainService.getSessionMain(data.getId());
           if (!SessionStatus.ACTIVE.name().equals(erSessionMeta.getStatus())) {
               logger.error("unable to start all processors for session id {} total {} active {} ", erSessionMeta.getId(),
                       erSessionMeta.getTotalProcCount(), erSessionMeta.getActiveProcCount());
               sessionManager.killSession(context, data);
               StringBuilder builder = new StringBuilder();
               builder.append("unable to start all processors for session id: . ")
                       .append(data.getId())
                       .append("total processors:").append(erSessionMeta.getTotalProcCount()).append(" \n")
                       .append("started count:").append(erSessionMeta.getActiveProcCount());
               throw new ErSessionException(builder.toString());

           }
       }
        return  sessionMainService.getSession(data.getId(),true,true,true);
    }

        private boolean checkSessionRpcReady(ErSessionMeta session) {

        long startTimeout = System.currentTimeMillis() + MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS;
        boolean isStarted = false;
        ErSessionMeta cur = null;
        while (System.currentTimeMillis() <= startTimeout) {
            cur = this.sessionMainService.getSession(session.getId(), false, false, false);
            if (cur == null) {
                return false;
            }
            if (cur.isOverState() || SessionStatus.ACTIVE.name().equals(cur.getStatus()))
                return true;

            if (SessionStatus.NEW.name().equals(cur.getStatus()) &&
                    ((cur.getActiveProcCount()==null || cur.getTotalProcCount() ==null) ||
                    (cur.getActiveProcCount() < cur.getTotalProcCount()))) {
                try {
                    logger.info("========waiting");
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                isStarted = true;
                break;
            }
        }
        return isStarted;
    }


}
