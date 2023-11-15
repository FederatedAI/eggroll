package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Inject;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionOptionService;
import org.fedai.eggroll.clustermanager.entity.SessionMain;
import org.fedai.eggroll.clustermanager.entity.SessionOption;
import org.fedai.eggroll.core.constant.SessionStatus;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractSessionStateHandler implements StateHandler<ErSessionMeta> {

    Logger logger = LoggerFactory.getLogger(AbstractSessionStateHandler.class);

    @Inject
    SessionMainService sessionMainService;
    @Inject
    ProcessorStateMachine processorStateMachine;
    @Inject
    ServerNodeService serverNodeService;
    @Inject
    SessionOptionService sessionOptionService;

    void updateStatus(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        SessionMain sessionMain = new SessionMain();
        sessionMain.setSessionId(erSessionMeta.getId());
        sessionMain.setStatus(desStateParam);
        sessionMain.setActiveProcCount(erSessionMeta.getActiveProcCount());
        sessionMain.setBeforeStatus(preStateParam);
        sessionMainService.updateById(sessionMain);
    }

    void doInserSession(Context context, ErSessionMeta erSessionMeta) {

        Map<String, String> options = erSessionMeta.getOptions();

        options.forEach((k, v) -> {
            SessionOption sessionOption = new SessionOption();
            sessionOption.setSessionId(erSessionMeta.getId());
            sessionOption.setName(k);
            sessionOption.setData(v);
            sessionOptionService.save(sessionOption);
        });

        erSessionMeta.getProcessors().forEach(p -> {
            processorStateMachine.changeStatus(context, p, null, SessionStatus.NEW.name());
        });

    }


}
