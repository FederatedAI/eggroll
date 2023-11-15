package org.fedai.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.clustermanager.entity.SessionMain;
import org.fedai.eggroll.core.constant.SessionStatus;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

@Singleton
public class SessionWaitResourceHandler extends AbstractSessionStateHandler {
    Logger logger = LoggerFactory.getLogger(SessionWaitResourceHandler.class);

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        int activeProcCount = 0;
        SessionMain sessionMain = new SessionMain(erSessionMeta.getId(), erSessionMeta.getName(), SessionStatus.WAITING_RESOURCE.name(),
                erSessionMeta.getTag(), erSessionMeta.getProcessors().size(), activeProcCount, new Date(), new Date());
        sessionMainService.save(sessionMain);
        return erSessionMeta;
    }


    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
    }
}
