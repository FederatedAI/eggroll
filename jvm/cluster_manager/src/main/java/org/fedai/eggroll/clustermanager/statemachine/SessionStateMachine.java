package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;


@Singleton
public class SessionStateMachine extends AbstractStateMachine<ErSessionMeta> {

    @Inject
    SessionMainService sessionMainService;

    @Inject
    ProcessorStateMachine processorStateMachine;

    @Inject
    SessionKillHandler sessionKillHandler;

    @Inject
    SessionStopHandler sessionStopHandler;

    @Inject
    SessionActiveHandler sessionActiveHandler;

    @Inject
    SessionCreateHandler sessionCreateHandler;
    @Inject
    SessionWaitResourceHandler sessionWaitResourceHandler;
    @Inject
    SessionWaitNewHandler sessionWaitNewHandler;
    @Inject
    SessionWaitingKillHandler sessionWaitingKillHandler;
    @Inject
    SessionIgnoreHandler sessionIgnoreHandler;
    @Inject
    SessionRepeatedCreateHandler sessionRepeatedCreateHandler;

    @Override
    String buildStateChangeLine(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        String line = "";
        ErSessionMeta sessionInDb = sessionMainService.getSession(erSessionMeta.getId(), true, false, false);
        if (sessionInDb != null) {
            context.putData(Dict.SESSION_IN_DB, sessionInDb);
        }
        if (StringUtils.isEmpty(preStateParam)) {
            if (sessionInDb == null) {
                preStateParam = "";
            } else {
                preStateParam = sessionInDb.getStatus();
            }
        }
        line = preStateParam + "_" + desStateParam;
        context.putLogData("session_status_change", line);
        context.putData(Dict.BEFORE_STATUS,preStateParam);
        return line;
    }

    @Override
    public String getLockKey(Context context, ErSessionMeta erSessionMeta) {
        return erSessionMeta.getId();
    }

    @Inject
    public void afterPropertiesSet() throws Exception {
        this.registeStateHander("_WAITING_RESOURCE", sessionWaitResourceHandler);
        this.registeStateHander("WAITING_RESOURCE_NEW", sessionWaitNewHandler);
        this.registeStateHander("_NEW", sessionCreateHandler);
        this.registeStateHander("NEW_NEW", sessionRepeatedCreateHandler);
        this.registeStateHander("NEW_ACTIVE", sessionActiveHandler);
        this.registeStateHander("NEW_KILLED", sessionKillHandler);
        this.registeStateHander("NEW_ERROR", sessionKillHandler);
        this.registeStateHander("ACTIVE_KILLED", sessionKillHandler);
        this.registeStateHander("ACTIVE_ERROR", sessionKillHandler);
        this.registeStateHander("ACTIVE_CLOSED", sessionStopHandler);
        this.registeStateHander("WAITING_RESOURCE_KILLED", sessionWaitingKillHandler);
        this.registeStateHander("WAITING_RESOURCE_ERROR", sessionWaitingKillHandler);
        this.registeStateHander("WAITING_RESOURCE_CLOSED", sessionWaitingKillHandler);
        this.registeStateHander(IGNORE, sessionIgnoreHandler);
    }

}
