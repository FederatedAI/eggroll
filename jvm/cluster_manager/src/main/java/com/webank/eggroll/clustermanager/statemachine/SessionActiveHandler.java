package com.webank.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.session.DefaultSessionManager;


@Singleton
public class SessionActiveHandler extends AbstractSessionStateHandler {
    @Inject
    DefaultSessionManager sessionManager;

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {

        logger.info("SessionActiveHandler=========prepre {}", data.getId());

        ErSessionMeta erSessionMeta = sessionMainService.getSession(data.getId(), false, false, false);

        if (!erSessionMeta.getStatus().equals(SessionStatus.NEW.name())) {

            setIsBreak(context, true);
        } else {
            logger.info("SessionActiveHandler=========openAsynPostHandle  {}", data.getId());
            openAsynPostHandle(context);
        }
        erSessionMeta.setStatus(SessionStatus.ACTIVE.name());
        return erSessionMeta;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        logger.info("handle session state {} to {}", preStateParam, desStateParam);
        this.sessionMainService.update(new LambdaUpdateWrapper<SessionMain>().eq(SessionMain::getSessionId, data.getId()).set(SessionMain::getStatus, data.getStatus()));
        // .set(SessionMain::getActiveProcCount,data.getActiveProcCount()));
        return this.sessionMainService.getSession(data.getId(), false, false, false);
    }


    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        // 唤醒阻塞进程

        sessionManager.wakeUpSession(data.getId());
    }

    ;


}
