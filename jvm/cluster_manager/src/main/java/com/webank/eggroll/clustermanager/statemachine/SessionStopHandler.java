package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Singleton;
import org.springframework.stereotype.Service;

@Service
@Singleton
public class SessionStopHandler extends AbstractSessionStateHandler{
    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return null;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return null;
    }



    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {

    }
}
