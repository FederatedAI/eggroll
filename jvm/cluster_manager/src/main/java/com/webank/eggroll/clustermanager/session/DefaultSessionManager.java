package com.webank.eggroll.clustermanager.session;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;


import com.webank.eggroll.clustermanager.statemechine.StateMachine;
import com.webank.eggroll.core.meta.ErProcessor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DefaultSessionManager implements SessionManager{

    Logger logger = LoggerFactory.getLogger(DefaultSessionManager.class);
    @Autowired
    SessionMainService sessionService;



    StateMachine  sessionStateMachine;





    @Override
    public com.eggroll.core.pojo.ErProcessor heartbeat(com.eggroll.core.pojo.ErProcessor proc) {
        return null;
    }

    @Override
    public ErSessionMeta getSessionMain(String sessionId) {
        return null;
    }

    @Override
    public ErSessionMeta getOrCreateSession(ErSessionMeta sessionMeta) {

            return  null;

    }

    @Override
    public ErSessionMeta getSession(ErSessionMeta sessionMeta) {

        //logDebug(s"SESSION getSession: ${sessionMeta}")
        ErSessionMeta result =null;
        //val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
        long  startTimeout = System.currentTimeMillis()+ 20000;
        while (System.currentTimeMillis() <= startTimeout) {
           result = sessionService.getSession(sessionMeta.getId(),true);
                if (result != null && !result.getStatus().equals(SessionStatus.NEW.name()) && !StringUtils.isBlank(result.getId())) {
                    break;
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        return  result;
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

        ErSessionMeta erSessionMeta = sessionService.getSession(sessionMeta.getId(),true);
        if(erSessionMeta==null){
            return null;
        }






//        def killSession(sessionMeta: ErSessionMeta, afterState: String): ErSessionMeta = {
//                val sessionId = sessionMeta.id
//        if (!smDao.existSession(sessionId)) {
//            return null
//        }
//        val dbSessionMeta = smDao.getSession(sessionId)
//        if (StringUtils.equalsAny(dbSessionMeta.status, SessionStatus.KILLED, SessionStatus.CLOSED, SessionStatus.ERROR)) {
//            return dbSessionMeta
//        }
//        if(dbSessionMeta.processors.length>0) {
//            val sessionHosts = dbSessionMeta.processors.filter(p=>p.commandEndpoint!=null&&StringUtils.isNotEmpty(p.commandEndpoint.host))
//        .map(p => p.commandEndpoint.host).toSet
//            if(!sessionHosts.isEmpty) {
//                val serverNodeCrudOperator = new ServerNodeCrudOperator()
//                val sessionServerNodes = serverNodeCrudOperator.getServerClusterByHosts(sessionHosts.toList.asJava).serverNodes
//
//                sessionServerNodes.par.foreach(n => {
//                        // TODO:1: add new params?
//                        val newSessionMeta = dbSessionMeta.copy(
//                        options = dbSessionMeta.options ++ Map(ResourceManagerConfKeys.SERVER_NODE_ID -> n.id.toString))
//                val nodeManagerClient = new NodeManagerClient(
//                        ErEndpoint(host = n.endpoint.host,
//                                port = n.endpoint.port))
//                nodeManagerClient.killContainers(newSessionMeta)
//        })
//            }
//        }
//
//        // todo:1: update selective
//        smDao.updateSessionMain(dbSessionMeta.copy(activeProcCount = 0, status = afterState),afterCall=ProcessorStateMachine.defaultSessionCallback)
//        var  resultSession = getSession(dbSessionMeta)
//        resultSession
//    }




        return null;
    }

    @Override
    public ErSessionMeta killAllSessions(ErSessionMeta sessionMeta) {
        return null;
    }




}
