package com.webank.eggroll.clustermanager.session;

import com.webank.eggroll.clustermanager.dao.impl.SessionServiceNew;
import com.webank.eggroll.core.meta.ErProcessor;
import com.webank.eggroll.core.meta.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DefaultSessionManager implements SessionManager{

    Logger logger = LoggerFactory.getLogger(DefaultSessionManager.class);
    @Autowired
    SessionServiceNew sessionServiceNew;



    @Override
    public ErProcessor heartbeat(ErProcessor proc) {
        return null;
    }

    @Override
    public ErSessionMeta getSessionMain(String sessionId) {
        return null;
    }

    @Override
    public ErSessionMeta getOrCreateSession(ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta getSession(ErSessionMeta sessionMeta) {

        //logDebug(s"SESSION getSession: ${sessionMeta}")
        var result: ErSessionMeta = null
        val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
        // todo:1: use retry framework
        breakable {
            while (System.currentTimeMillis() <= startTimeout) {



                result = smDao.getSession(sessionMeta.id)
                if (result != null && !result.status.equals(SessionStatus.NEW) && !StringUtils.isBlank(result.id)) {
                    break()
                } else {
                    Thread.sleep(100)
                }
            }
        }

        result


        return null;
    }

    @Override
    public ErSessionMeta registerSession(ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta stopSession(ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta killSession(ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta killSession(ErSessionMeta sessionMeta, String afterState) {
        return null;
    }

    @Override
    public ErSessionMeta killAllSessions(ErSessionMeta sessionMeta) {
        return null;
    }
}
