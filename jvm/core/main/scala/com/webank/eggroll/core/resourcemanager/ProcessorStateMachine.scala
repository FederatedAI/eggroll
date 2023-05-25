package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.SessionConfKeys.EGGROLL_SESSION_USE_RESOURCE_DISPATCH
import com.webank.eggroll.core.constant.{ProcessorTypes, ResourceStatus, SessionStatus}
import com.webank.eggroll.core.error.{ErProcessorException, ErSessionException}
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.SessionManagerService.smDao
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import java.sql.Connection

object ProcessorStateMachine extends Logging{

//  object ProcessorStatus {
//    val NEW = "NEW"
//    val RUNNING = "RUNNING"
//    val STOPPED = "STOPPED"
//    val KILLED = "KILLED"
//    val ERROR = "ERROR"
//  }


  private val smDao = new SessionMetaDao

  private var serverNodeCrudOperator = new  ServerNodeCrudOperator;

  def  changeStatus(  erProcessor: ErProcessor,
                      preStateParam:String=null,desStateParam:String,connection: Connection=null ):Unit =synchronized{
        var  preState=preStateParam
        var  processorType = erProcessor.processorType
        if(preState==null){
        var processorsInDb =   serverNodeCrudOperator.queryProcessor(connection,erProcessor.copy(status=null))
          if(processorsInDb.length==0){
              logError(s"can not found processor , ${erProcessor}")
              throw new ErProcessorException(s"can not found processor id ${erProcessor.id}")
          }else{
            preState = processorsInDb.apply(0).status
            processorType = processorsInDb.apply(0).processorType
          }
        }
        var statusLine =  preState+"_"+desStateParam;
        logInfo(s"==========statusLine================${statusLine} === =========${erProcessor}")
        var desErProcessor = erProcessor.copy(status = desStateParam)

        statusLine match {
          case "NEW_RUNNING"=>updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{

            if(EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()=="true"||processorType=="DeepSpeed")
              ResourceStateMachine.changeState(conn, Array(erProcessor), ResourceStatus.PRE_ALLOCATED, ResourceStatus.ALLOCATED)
            else{
              logInfo(s"resource dispatch config is ${EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()}, processor ${erProcessor.id}  without resource change, ")
            }
          })
          case statusLine if(statusLine=="NEW_STOPPED"||statusLine=="NEW_KILLED"||statusLine=="NEW_ERROR") =>updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
            if(EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()=="true"||processorType=="DeepSpeed")
                ResourceStateMachine.changeState(conn,Array(erProcessor),ResourceStatus.PRE_ALLOCATED,ResourceStatus.ALLOCATE_FAILED)
            else{
              logInfo(s"resource dispatch config is ${EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()}, processor ${erProcessor.id}  without resource change, ")
            }
          })
          case statusLine if(statusLine=="RUNNING_FINISHED"||statusLine=="RUNNING_STOPPED"||statusLine=="RUNNING_KILLED"||statusLine=="RUNNING_ERROR")=>
            updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
              if(EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()=="true"||processorType=="DeepSpeed")
                  ResourceStateMachine.changeState(conn,Array(erProcessor),ResourceStatus.ALLOCATED,ResourceStatus.RETURN)
              else{
                logInfo(s"resource dispatch config is ${EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()}, processor ${erProcessor.id}  without resource change, ")
              }
            })
          case _=> println("============error status=============");
        }
    }

  private def  updateState(erProcessor: ErProcessor,connection: Connection,beforeCall:(Connection,ErProcessor)=>Unit =null,afterCall:(Connection,ErProcessor)=>Unit): Unit ={



    if(connection==null){
      BaseDao.dbc.withTransaction(conn=> {
        if(beforeCall!=null)
          beforeCall(conn,erProcessor)
        smDao.updateProcessor(conn,erProcessor)
        if(afterCall!=null)
          afterCall(conn,erProcessor)
      })
    }else{
      if(beforeCall!=null)
        beforeCall(connection,erProcessor)
      smDao.updateProcessor(connection,erProcessor)
      if(afterCall!=null)
        afterCall(connection,erProcessor)
    }

  }


  def  updateAndReturnResource(proc: ErProcessor): Unit ={
    ClusterResourceManager.returnResource(beforeCall=  (conn,proc) =>smDao.updateProcessor(conn, proc),processors =Array(proc))
  }


  def  updateAndAllocateResource(proc:ErProcessor): Unit ={
    ClusterResourceManager.allocateResource(beforeCall=  (conn,proc) =>smDao.updateProcessor(conn, proc),processors =Array(proc))
  }

 def  defaultSessionCallback  (conn:Connection ,erSessionMeta: ErSessionMeta  ):Unit={
   //if(EGGROLL_SESSION_USE_RESOURCE_DISPATCH.get()=="true"||erSessionMeta.name=="DeepSpeed") {
     erSessionMeta.processors.foreach(p=>{
        ProcessorStateMachine.changeStatus(p,desStateParam =erSessionMeta.status,connection = conn )
     })
   //}
 }









}
