package com.webank.eggroll.clustermanager.session;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ServerNodeStatus;
import com.eggroll.core.constant.ServerNodeTypes;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.exceptions.ErSessionException;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.*;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.statemachine.SessionStateMachine;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Singleton
@Data
public class DefaultSessionManager implements SessionManager {

    Logger logger = LoggerFactory.getLogger(DefaultSessionManager.class);
    @Inject
    SessionMainService sessionService;
    @Inject
    SessionStateMachine sessionStateMachine;
    @Inject
    ServerNodeService serverNodeService;
    @Inject
    SessionMainService sessionMainService;
    @Inject
    ClusterResourceManager clusterResourceManager;

    public  void blockSession(ErSessionMeta erSessionMeta) throws InterruptedException {
        SessionLatchHolder  holder =  waitingMap.get(erSessionMeta.getId());
        if(holder==null){
            waitingMap.putIfAbsent(erSessionMeta.getId(),new SessionLatchHolder());
        }
        holder =  waitingMap.get(erSessionMeta.getId());
        CountDownLatch latch = new CountDownLatch(1);
        holder.countDownLatchs.add(latch);
        latch.await(MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public void wakeUpSession(String sessionId){
        logger.info("wake up session {} block thread ",sessionId);
        if(waitingMap.get(sessionId)!=null){
            waitingMap.get(sessionId).countDownLatchs.forEach(countDownLatch -> countDownLatch.countDown());
            waitingMap.remove(sessionId);
        }else{
            logger.warn("wake up session {} block thread not found ",sessionId);
        }
    }

    ConcurrentHashMap<String,SessionLatchHolder>  waitingMap = new ConcurrentHashMap<String,SessionLatchHolder>();

    class SessionLatchHolder{
        long createTimestamp=System.currentTimeMillis();
        List<CountDownLatch>  countDownLatchs= Lists.newArrayList();
    }

    @Override
    public ErSessionMeta getSessionMain(String sessionId) {
        return null;
    }

    @Override
    public ErSessionMeta getOrCreateSession(Context context, ErSessionMeta sessionMeta) {
        context.setSessionId(sessionMeta.getId());
        context.setOptions(sessionMeta.getOptions());
      //  if (MetaInfo.EGGROLL_SESSION_USE_RESOURCE_DISPATCH) {
        return getOrCreateSessionWithoutResourceDispath(context, sessionMeta);
//        } else {
////            getOrCreateSessionOld(sessionMeta);
//        }


    }

    private ErSessionMeta getOrCreateSessionWithoutResourceDispath(Context context, ErSessionMeta sessionMeta) {
        ErSessionMeta newSession = sessionStateMachine.changeStatus(context, sessionMeta, null, SessionStatus.NEW.name());
        logger.info("new session======={}",newSession);
        if (!SessionStatus.NEW.name().equals(newSession.getStatus())) {
            return newSession;
        }
        try {
            this.blockSession(sessionMeta);
        } catch (InterruptedException e) {
            ErSessionMeta erSessionMeta=  this.sessionMainService.getSessionMain(sessionMeta.getId());
            if(!SessionStatus.ACTIVE.name().equals(erSessionMeta.getStatus())){
                logger.error("unable to start all processors for session id {} total {} active {} ",erSessionMeta.getId(),
                        erSessionMeta.getTotalProcCount(),erSessionMeta.getActiveProcCount());
                killSession(context,sessionMeta);
                StringBuilder builder = new StringBuilder();
                builder.append("unable to start all processors for session id: . ")
                        .append(sessionMeta.getId())
                        .append("total processors:").append(erSessionMeta.getTotalProcCount()) .append(" \n")
                        .append("started count:").append(erSessionMeta.getActiveProcCount());
                throw new ErSessionException(builder.toString());
            }
        }
        return  sessionService.getSession(sessionMeta.getId(),true,true,true);

//        if (checkSessionRpcReady(newSession)) {
//            return sessionStateMachine.changeStatus(context, newSession, SessionStatus.NEW.name(), SessionStatus.ACTIVE.name());
//        } else {
//            return sessionStateMachine.changeStatus(context, newSession, SessionStatus.NEW.name(), SessionStatus.ERROR.name());
//        }

    }



//    private boolean checkSessionRpcReady(ErSessionMeta session) {
//
//        long startTimeout = System.currentTimeMillis() + MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS;
//        boolean isStarted = false;
//        ErSessionMeta cur = null;
//        while (System.currentTimeMillis() <= startTimeout) {
//            cur = this.sessionService.getSession(session.getId(), false, false, false);
//            if (cur == null) {
//                return false;
//            }
//            logger.info("=======cur session {}",cur);
//
//            if (cur.isOverState() || SessionStatus.ACTIVE.name().equals(cur.getStatus()))
//                return true;
//
//
//
//            if (SessionStatus.NEW.name().equals(cur.getStatus()) &&
//                    ((cur.getActiveProcCount()==null || cur.getTotalProcCount() ==null) ||
//                    (cur.getActiveProcCount() < cur.getTotalProcCount()))) {
////                try {
////                    logger.info("========waiting");
////                    Thread.sleep(100);
////                } catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//                if(waitingMap.putIfAbsent())
//
//
//            } else {
//                isStarted = true;
//                break;
//            }
//        }
//        return isStarted;
//
//
//    }

    private List<ErServerNode> getHealthServerNode() {
        ErServerNode serverNodeExample = new ErServerNode();
        serverNodeExample.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        serverNodeExample.setStatus(ServerNodeStatus.HEALTHY.name());
        int tryCount = 0;
        do {
            List<ErServerNode> healthyServerNodes = serverNodeService.getListByErServerNode(serverNodeExample);
            tryCount += 1;
            if (healthyServerNodes.size() == 0) {
                logger.info("cluster is not ready,waitting next try");
                try {
                    Thread.sleep(MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                return healthyServerNodes;
            }
        }
        while (tryCount < 2);
        return Lists.newArrayList();
    }


    @Override
    public ErSessionMeta getSession(Context context, ErSessionMeta sessionMeta) {
//        checkSessionRpcReady(sessionMeta);
        return sessionService.getSession(sessionMeta.getId(), true, false, false);
    }

    @Override
    public ErSessionMeta registerSession(Context context, ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta stopSession(Context context, ErSessionMeta sessionMeta) {
        return sessionStateMachine.changeStatus(context, sessionMeta, null, SessionStatus.CLOSED.name());
    }

    @Override
    public ErSessionMeta killSession(Context context, String sessionId) {
        ErSessionMeta sessionMeta = sessionMainService.getSession(sessionId);
        return sessionStateMachine.changeStatus(context, sessionMeta, null, SessionStatus.KILLED.name());
    }

    @Override
    public ErSessionMeta killSession(Context context, ErSessionMeta sessionMeta) {
        return sessionStateMachine.changeStatus(context, sessionMeta, null, SessionStatus.KILLED.name());

    }

    @Override
    public ErSessionMeta killSession(Context context, ErSessionMeta sessionMeta, String afterState) {
        return sessionStateMachine.changeStatus(context, sessionMeta, null, afterState);
    }


    @Override
    public ErSessionMeta killAllSessions(Context context, ErSessionMeta sessionMeta) {
        QueryWrapper<SessionMain> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda().in(SessionMain::getStatus, Arrays.asList(SessionStatus.NEW.name(), SessionStatus.ACTIVE.name()));
        List<SessionMain> killList = sessionMainService.list(queryWrapper);
        for (SessionMain sessionMain : killList) {
            ErSessionMeta erSessionMeta = sessionMain.toErSessionMeta();
            try {
                sessionStateMachine.changeStatus(context, erSessionMeta, null, SessionStatus.KILLED.name());
            }catch (Exception e) {
                logger.error("kill session failed , sessionId = {}",erSessionMeta);
            }
        }
        return new ErSessionMeta();
    }


}
