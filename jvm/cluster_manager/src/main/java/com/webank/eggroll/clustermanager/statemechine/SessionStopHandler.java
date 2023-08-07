package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import org.springframework.stereotype.Service;

@Service
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
