package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.SessionConfKeys.EGGROLL_SESSION_USE_RESOURCE_DISPATCH
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, ProcessorTypes, ResourceStatus, SessionStatus}
import com.webank.eggroll.core.error.{ErProcessorException, ErSessionException}
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.SessionManagerService.smDao
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging

import java.sql.Connection

object ProcessorStateMachine extends Logging{

  private val smDao = new SessionMetaDao

  private var serverNodeCrudOperator = new  ServerNodeCrudOperator;

  def  changeStatus(  paramProcessor: ErProcessor,
                      preStateParam:String=null,desStateParam:String,connection: Connection=null ):Unit =synchronized{
        var  erProcessor = paramProcessor
        var  beginTimeStamp = System.currentTimeMillis()
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

        var desErProcessor = erProcessor.copy(status = desStateParam)
        var dispatchConfig =StaticErConf.getProperty(EGGROLL_SESSION_USE_RESOURCE_DISPATCH, "false")
        statusLine match {
          case "_NEW" =>

            erProcessor = createNewProcessor(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
            if( dispatchConfig=="true"||processorType=="DeepSpeed")
              ResourceStateMachine.changeState(conn, Array(erProcessor), ResourceStatus.INIT, ResourceStatus.PRE_ALLOCATED)
          })

          case "NEW_RUNNING"=>updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
            if( dispatchConfig=="true"||processorType=="DeepSpeed")
              ResourceStateMachine.changeState(conn, Array(erProcessor), ResourceStatus.PRE_ALLOCATED, ResourceStatus.ALLOCATED)
          })
          case statusLine if(statusLine=="NEW_STOPPED"||statusLine=="NEW_KILLED"||statusLine=="NEW_ERROR") =>updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
            if(  dispatchConfig=="true"||processorType=="DeepSpeed")
                ResourceStateMachine.changeState(conn,Array(erProcessor),ResourceStatus.PRE_ALLOCATED,ResourceStatus.ALLOCATE_FAILED)
          })
          case statusLine if(statusLine=="RUNNING_FINISHED"||statusLine=="RUNNING_STOPPED"||statusLine=="RUNNING_KILLED"||statusLine=="RUNNING_ERROR")=>
            updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
              if(dispatchConfig=="true"||processorType=="DeepSpeed")
                  ResourceStateMachine.changeState(conn,Array(erProcessor),ResourceStatus.ALLOCATED,ResourceStatus.RETURN)
            })
          case _=> logInfo(s"there is no need to do something with ${erProcessor.id} state ${statusLine}");
        }
    if( dispatchConfig=="false"&&processorType != "DeepSpeed"){
      logInfo(s"resource dispatch config is ${dispatchConfig}, processor ${erProcessor.id}  without resource change, ")
    }
    logInfo(s"processor ${erProcessor.id} change status ${statusLine} cost time ${System.currentTimeMillis()-beginTimeStamp}")
    }


  private def  createNewProcessor(erProcessor: ErProcessor,connection: Connection,beforeCall:(Connection,ErProcessor)=>Unit =null,afterCall:(Connection,ErProcessor)=>Unit): ErProcessor ={
    if(connection==null){
      BaseDao.dbc.withTransaction(conn=> {
        if(beforeCall!=null)
          beforeCall(conn,erProcessor)
        var  newProcessor = smDao.createProcessor(conn,erProcessor)
        if(afterCall!=null)
          afterCall(conn,newProcessor)
        newProcessor
      })
    }else{
      if(beforeCall!=null)
        beforeCall(connection,erProcessor)
      var  newProcessor = smDao.createProcessor(connection,erProcessor)
      logInfo(s"create new processor =========${newProcessor}");
      if(afterCall!=null)
        afterCall(connection,newProcessor)

      newProcessor
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
