package com.webank.eggroll.clustermanager.session;

import com.webank.eggroll.core.meta.ErProcessor;
import com.webank.eggroll.core.meta.ErSessionMeta;
import org.springframework.stereotype.Service;


public interface SessionManager {


    ErProcessor heartbeat(ErProcessor proc);

    ErSessionMeta getSessionMain(String sessionId);
    /**
     * get or create session
     * @param sessionMeta session main and options
     * @return session main and options and processors
     */
    ErSessionMeta getOrCreateSession(ErSessionMeta sessionMeta );

    /**
     * get session detail
     * @param sessionMeta contains session id
     * @return session main and options and processors
     */
    ErSessionMeta getSession(ErSessionMeta sessionMeta );

    /**
     * register session without boot processors
     * @param sessionMeta contains session main and options and processors
     * @return
     */
    ErSessionMeta registerSession(ErSessionMeta sessionMeta);

    ErSessionMeta stopSession(ErSessionMeta sessionMeta );

    ErSessionMeta  killSession( ErSessionMeta sessionMeta);

    ErSessionMeta killSession(ErSessionMeta sessionMeta ,String afterState );

    ErSessionMeta killAllSessions( ErSessionMeta sessionMeta);


}
