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
    def getOrCreateSession(sessionMeta: ErSessionMeta): ErSessionMeta

    /**
     * get session detail
     * @param sessionMeta contains session id
     * @return session main and options and processors
     */
    def getSession(sessionMeta: ErSessionMeta): ErSessionMeta

    /**
     * register session without boot processors
     * @param sessionMeta contains session main and options and processors
     * @return
     */
    def registerSession(sessionMeta: ErSessionMeta): ErSessionMeta

    def stopSession(sessionMeta: ErSessionMeta): ErSessionMeta

    def killSession(sessionMeta: ErSessionMeta): ErSessionMeta

    def killSession(sessionMeta: ErSessionMeta, afterState: String): ErSessionMeta

    def killAllSessions(sessionMeta: ErSessionMeta): ErSessionMeta
}

}
