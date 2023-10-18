package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Singleton
public class SessionIgnoreHandler extends  AbstractSessionStateHandler{
    Logger logger = LoggerFactory.getLogger(SessionIgnoreHandler.class);
    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        logger.warn("session {} receive invalid state {} to {} ",data.getId(),preStateParam,desStateParam);
        return data;
    }
}
