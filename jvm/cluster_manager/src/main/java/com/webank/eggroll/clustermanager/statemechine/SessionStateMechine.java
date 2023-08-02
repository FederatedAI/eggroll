package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.exceptions.ErSessionException;
import com.eggroll.core.pojo.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class SessionStateMechine {


//    @Autowired
//    SessionServiceNew sessionServiceNew;




    Logger logger = LoggerFactory.getLogger(SessionStateMechine.class);

    private ConcurrentHashMap<String, ReentrantLock>  sessionLockMap = new ConcurrentHashMap<String, ReentrantLock>();

    public void  tryLock(String sessionId ){
        ReentrantLock  lock  = null;
        if(sessionLockMap.contains(sessionId)){
            lock = sessionLockMap.get(sessionId);
        }else{
            sessionLockMap.putIfAbsent(sessionId,new ReentrantLock());
            lock  = sessionLockMap.get(sessionId);
        }
        lock.lock();
    }

    public void unLock(String sessionId ){
        ReentrantLock  lock = sessionLockMap.get(sessionId);
        if(lock!=null){
            lock.unlock();
            sessionLockMap.remove(sessionId);
        }
    }

   public  void  changeStatus( ErSessionMeta paramErSession,
                                 String preStateParam,
                                 String desStateParam,
                                 SessionChecker preCheck
            ){
        Long  beginTimeStamp = System.currentTimeMillis();
        try {
            tryLock(paramErSession.getId());
            if(preCheck!=null){
                if(!preCheck.check(paramErSession)){
                    throw  new ErSessionException("");
                }
            }
            String preState = preStateParam;
            if (preState == null) {
                preState = "";
            }
            String statusLine = preState + "_" + desStateParam;

            switch(statusLine){
                case  "_NEW" :
                   // sessionServiceNew.registerSession(paramErSession);



                    break;


                case "NEW_ACTIVE" : break;
                case "ACTIVE_CLOSED" :;
                case "ACTIVE_KILLED" :;
                case "ACTIVE_ERROR" :;
                case "ACTIVE_FINISHED" : break;


                case "NEW_STOPPED":;
                case "NEW_KILLED":;
                case "NEW_ERROR": break;
                default:

            }



//            statusLine match {
//                case "_NEW" =>
//                    smDao.registerWithResourceV2(paramErSession)
//                case "NEW_ACTIVE" =>
//                    //NEW_ACTIVE 意味着所有算子已就绪，所以不需要操作资源
//                    smDao.updateSessionMain(paramErSession)
//                case statusLine if (statusLine == "ACTIVE_CLOSED" || statusLine == "ACTIVE_KILLED" || statusLine == "ACTIVE_ERROR" || statusLine == "ACTIVE_FINISHED") =>
//                    smDao.updateSessionMain(paramErSession, afterCall = processorStateMachine.defaultSessionCallback)
//                case statusLine if (statusLine == "NEW_STOPPED" || statusLine == "NEW_KILLED" || statusLine == "NEW_ERROR" || statusLine == "ACTIVE_FINISHED") =>
//                    smDao.updateSessionMain(paramErSession, afterCall = processorStateMachine.defaultSessionCallback)
//                case _ => logInfo(s"there is no need to do something with ${paramErSession.id} state ${statusLine}");
//            }
        }finally {
            unLock(paramErSession.getId());
        }
    }
    @Transactional
    private void createNewSession(ErSessionMeta  erSessionMeta){

    }


}
