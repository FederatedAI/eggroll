package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;

public class SessionActiveHandler extends AbstractSessionStateHandler{
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
        super.asynPostHandle(context, data, preStateParam, desStateParam);
    }
}
