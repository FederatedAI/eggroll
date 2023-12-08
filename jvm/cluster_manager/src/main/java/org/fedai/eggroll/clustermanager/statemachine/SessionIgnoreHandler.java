package org.fedai.eggroll.clustermanager.statemachine;

import com.google.inject.Singleton;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SessionIgnoreHandler extends AbstractSessionStateHandler {
    Logger logger = LoggerFactory.getLogger(SessionIgnoreHandler.class);

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        logger.warn("session {} receive invalid state {} to {} ", data.getId(), preStateParam, desStateParam);
        return data;
    }
}
