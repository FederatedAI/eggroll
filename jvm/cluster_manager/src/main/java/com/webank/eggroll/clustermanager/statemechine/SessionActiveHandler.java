package com.webank.eggroll.clustermanager.statemechine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.entity.SessionMain;

public class SessionActiveHandler extends AbstractSessionStateHandler{



    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
//        ErSessionMeta  erSessionMeta = this.sessionMainService.getSession(data.getId(), true,false,false);
//        if(Ses)
        ErSessionMeta  erSessionMeta = (ErSessionMeta)context.getData(Dict.SESSION_IN_DB);
        int activeCount = 0;
        for (ErProcessor erProcessor : erSessionMeta.getProcessors()) {
            if (ProcessorStatus.RUNNING.name().equals(erProcessor.getStatus())) {
                    activeCount = activeCount + 1;
                }
        }
        erSessionMeta.setActiveProcCount(activeCount);
        if (erSessionMeta.getTotalProcCount() == activeCount) {
            erSessionMeta.setStatus(ProcessorStatus.RUNNING.name());
        }
        return  erSessionMeta;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        this.sessionMainService.update(new LambdaUpdateWrapper<SessionMain>().eq(SessionMain::getSessionId,data.getId()).set(SessionMain::getStatus,data.getStatus())
                .set(SessionMain::getActiveProcCount,data.getActiveProcCount()));
        return  data;
    }




}
