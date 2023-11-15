package com.webank.eggroll.clustermanager.session;


import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.fedai.eggroll.core.pojo.QueueViewResponse;


public interface SessionManager {


    ErSessionMeta getSessionMain(String sessionId);

    /**
     * get or create session
     *
     * @param sessionMeta session main and options
     * @return session main and options and processors
     */
    ErSessionMeta getOrCreateSession(Context context, ErSessionMeta sessionMeta);

    /**
     * get session detail
     *
     * @param sessionMeta contains session id
     * @return session main and options and processors
     */
    ErSessionMeta getSession(Context context, ErSessionMeta sessionMeta);

    /**
     * register session without boot processors
     *
     * @param sessionMeta contains session main and options and processors
     * @return
     */
    ErSessionMeta registerSession(Context context, ErSessionMeta sessionMeta);

    ErSessionMeta stopSession(Context context, ErSessionMeta sessionMeta);

    ErSessionMeta killSession(Context context, String sessionId);

    ErSessionMeta killSession(Context context, ErSessionMeta sessionMeta);

    ErSessionMeta killSession(Context context, ErSessionMeta sessionMeta, String afterState);

    ErSessionMeta killAllSessions(Context context, ErSessionMeta sessionMeta);

    QueueViewResponse getQueueView(Context context);

}
