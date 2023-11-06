package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.dao.impl.SessionOptionService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Date;
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
        int activeProcCount = 0;
        SessionMain sessionMain = new SessionMain(erSessionMeta.getId(), erSessionMeta.getName(), SessionStatus.NEW.name(),
                erSessionMeta.getTag(), erSessionMeta.getProcessors().size(), activeProcCount, new Date(), new Date());
        sessionMainService.save(sessionMain);
        Map<String, String> options = erSessionMeta.getOptions();

        options.forEach((k, v) -> {
            SessionOption sessionOption = new SessionOption();
            sessionOption.setSessionId(erSessionMeta.getId());
            sessionOption.setName(k);
            sessionOption.setData(v);
            sessionOptionService.save(sessionOption);
        });

        erSessionMeta.getProcessors().forEach(p -> {
//            logger.info("prepare to handle processor {}",p);
            processorStateMachine.changeStatus(context, p, null, SessionStatus.NEW.name());
        });

    }


}
