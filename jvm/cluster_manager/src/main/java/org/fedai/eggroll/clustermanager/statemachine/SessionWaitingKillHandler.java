package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.clustermanager.entity.SessionMain;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SessionWaitingKillHandler extends AbstractSessionStateHandler {
    Logger logger = LoggerFactory.getLogger(SessionWaitingKillHandler.class);

    @Inject
    SessionMainService sessionMainService;

    @Inject
    ServerNodeService serverNodeService;

    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
    }

    ;

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        SessionMain sessionMain = new SessionMain();
        sessionMain.setSessionId(erSessionMeta.getId());
        sessionMain.setStatus(desStateParam);
        sessionMain.setStatusReason(erSessionMeta.getStatusReason());
        erSessionMeta.setBeforeStatus(preStateParam);
        sessionMainService.updateById(sessionMain);
        return erSessionMeta;
    }
}
