package org.fedai.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.fedai.eggroll.clustermanager.entity.SessionMain;
import org.fedai.eggroll.clustermanager.session.DefaultSessionManager;
import org.fedai.eggroll.core.constant.SessionStatus;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.google.inject.Singleton;


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
        return this.sessionMainService.getSession(data.getId(), false, false, false);
    }


    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        // 唤醒阻塞进程
        sessionManager.wakeUpSession(data.getId());
    }

    ;


}
