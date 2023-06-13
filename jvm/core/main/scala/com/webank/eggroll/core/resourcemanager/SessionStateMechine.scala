package com.webank.eggroll.core.resourcemanager
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.SessionStatus
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta.ErSessionMeta
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.smDao
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
object SessionStateMechine extends Logging{

  private lazy val smDao = new SessionMetaDao
  private val  processorStateMachine =   ProcessorStateMachine
  private var  sessionLockMap = new ConcurrentHashMap[String,ReentrantLock]()

   def  tryLock(sessionId :String): Unit ={
     var  lock : ReentrantLock= null;
      if(sessionLockMap.contains(sessionId)){
        lock = sessionLockMap.get(sessionId)
      }else{
        sessionLockMap.putIfAbsent(sessionId,new ReentrantLock())
        lock  = sessionLockMap.get(sessionId)
      }
      lock.lock()
   }

   def  unLock(sessionId: String):Unit= {
      var  lock = sessionLockMap.get(sessionId)
     if(lock!=null){
        lock.unlock()
        sessionLockMap.remove(sessionId)
      }
   }


  def  checkSessionIdNotExistInDb(paramErSession: ErSessionMeta):Boolean ={
      smDao.existSession(paramErSession.id)
  }

  def checkSesssionStatusEquals(paramErSession: ErSessionMeta):Boolean ={
       var sessionInDb = smDao.getSession(paramErSession.id)
      if(ErSessionMeta==null){
          false
      }else{
        if(sessionInDb.status==paramErSession.status)
             true
        else
            false
      }
  }



   def  changeStatus( paramErSession: ErSessionMeta,
                      preStateParam:String=null,
                      desStateParam:String,
                      preCheck:( ErSessionMeta)=>Boolean=null


                    ):Unit = {
     var beginTimeStamp = System.currentTimeMillis()
     try {
       tryLock(paramErSession.id)
       if(preCheck!=null){
          if(!preCheck(paramErSession)){
               throw  new ErSessionException()
          }
       }
       var preState = preStateParam
       if (preState == null) {
         preState = ""
       }
       var statusLine = preState + "_" + desStateParam;
       statusLine match {
         case "_NEW" =>
           smDao.registerWithResourceV2(paramErSession)
         case "NEW_ACTIVE" =>
           //NEW_ACTIVE 意味着所有算子已就绪，所以不需要操作资源
           smDao.updateSessionMain(paramErSession)
         case statusLine if (statusLine == "ACTIVE_CLOSED" || statusLine == "ACTIVE_KILLED" || statusLine == "ACTIVE_ERROR" || statusLine == "ACTIVE_FINISHED") =>
           smDao.updateSessionMain(paramErSession, afterCall = processorStateMachine.defaultSessionCallback)
         case statusLine if (statusLine == "NEW_STOPPED" || statusLine == "NEW_KILLED" || statusLine == "NEW_ERROR" || statusLine == "ACTIVE_FINISHED") =>
           smDao.updateSessionMain(paramErSession, afterCall = processorStateMachine.defaultSessionCallback)
         case _ => logInfo(s"there is no need to do something with ${paramErSession.id} state ${statusLine}");
       }
     }finally {
       unLock(paramErSession.id)
     }
   }

}
