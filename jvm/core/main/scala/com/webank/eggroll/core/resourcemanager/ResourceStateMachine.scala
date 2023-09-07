package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.SessionConfKeys.EGGROLL_RESOURCE_SYSTEM_UPDATE_INTERVAL
import com.webank.eggroll.core.constant.{ResourceStatus, ResourceTypes, StringConstants}
import com.webank.eggroll.core.meta.{ErProcessor, ErResource}
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.{logInfo, serverNodeCrudOperator}
import com.webank.eggroll.core.resourcemanager.ProcessorStateMachine.smDao
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ResourceStateMachine extends Logging{


  var nodeResourceUpdateTimeMap :mutable.Map[Long,Long] = mutable.Map[Long,Long]()

  def  changeState(connection: Connection,processors: Array[ErProcessor],beforeState:String,afterState:String ): Unit =synchronized{
    if(connection==null){
      BaseDao.dbc.withTransaction(conn=> {
        changeStateInner(conn,processors,beforeState,afterState)
      })
    }else{
      changeStateInner(connection,processors,beforeState,afterState)
    }
  }


  private def   changeStateInner(connection: Connection,processors: Array[ErProcessor],beforeState:String,afterState:String ): Unit ={
    var  stateLine =  beforeState+"_"+afterState
    logInfo(s"resource change state ${stateLine}")
    stateLine match {
      case "init_pre_allocated" =>  preAllocateResource(connection,processors)
      case statusLine if(statusLine=="pre_allocated_allocated"||statusLine=="pre_allocated_allocate_failed"||statusLine=="allocated_return") =>
        updateResource(connection,processors,beforeState,afterState,afterCall = (conn,p)=>{
          countAndUpdateNodeResourceInner(conn,p.serverNodeId)
        })
      case _ => logError(s"there is no need do something with resource status ${stateLine}")
    }
  }

  def countAndUpdateNodeResource(serverNodeId: Long): Unit ={
    var lastUpdateTime:Long =nodeResourceUpdateTimeMap.getOrElse(serverNodeId,0)
    if(System.currentTimeMillis() > lastUpdateTime + EGGROLL_RESOURCE_SYSTEM_UPDATE_INTERVAL.get().toInt){
      BaseDao.dbc.withTransaction(conn=> {
        countAndUpdateNodeResourceInner(conn,serverNodeId)
      })

    }

  }

  private def  countAndUpdateNodeResourceInner(conn: Connection,serverNodeId: Long): Unit = synchronized{

    var  paramResources = new ArrayBuffer[ErResource]()
    var resourceList =  serverNodeCrudOperator.queryProcessorResourceList(conn,serverNodeId,Array(ResourceStatus.ALLOCATED ,ResourceStatus.PRE_ALLOCATED))
    if(resourceList.length>0) {
      resourceList.groupBy(_.resourceType).foreach(element => {
        var allocated: Long = 0
        var preAllocated: Long = 0
        var extensions: ArrayBuffer[String] = new ArrayBuffer[String]()

        element._2.foreach(r => {
          r.status match {
            case ResourceStatus.ALLOCATED =>
              allocated += r.allocated
              if (r.resourceType == ResourceTypes.VGPU_CORE && r.extention != StringConstants.EMPTY) {
                extensions.append(r.extention)
              }
            case ResourceStatus.PRE_ALLOCATED =>
              preAllocated += r.allocated
              if (r.resourceType == ResourceTypes.VGPU_CORE && r.extention != StringConstants.EMPTY) {
                extensions.append(r.extention)
              }
            case _ =>
          }
        })
        var mergedResource = ErResource(serverNodeId = serverNodeId,
          resourceType = element._1, preAllocated = preAllocated, allocated = allocated, extention = extensions.mkString(","))
        paramResources.append(mergedResource)
      })
    }else{

      paramResources.append( ErResource(serverNodeId = serverNodeId,
        resourceType = ResourceTypes.VGPU_CORE, preAllocated = 0, allocated = 0, extention = ""))
      paramResources.append(ErResource(serverNodeId = serverNodeId,
        resourceType = ResourceTypes.VCPU_CORE, preAllocated = 0, allocated = 0, extention = ""))
    }
    logInfo(s"try to update node resource ${serverNodeId} resource ${paramResources} ");

    serverNodeCrudOperator.updateNodeResource(conn, serverNodeId, paramResources.toArray)
    nodeResourceUpdateTimeMap.put(serverNodeId,System.currentTimeMillis())
  }



  private def preAllocateResource(conn : Connection,processors: Array[ErProcessor]): Unit = synchronized {

    //ServerNodeCrudOperator.dbc.withTransaction(conn => {

      serverNodeCrudOperator.insertProcessorResource(conn, processors);
    processors.groupBy(_.serverNodeId).foreach(e=>{
      countAndUpdateNodeResourceInner(conn,e._1)
    }
  )


  }


  private def  updateResource(conn: Connection,
                      processors: Array[ErProcessor],
                      beforeState:String,
                      afterState:String,
                      beforeCall:(Connection,ErProcessor)=>Unit =null,
                      afterCall:(Connection,ErProcessor)=>Unit =null):  Unit=synchronized{
      try {
        processors.foreach(p => {
          if (beforeCall != null) {
            beforeCall(conn,p)
          }
          var resourceInDb = serverNodeCrudOperator.queryProcessorResource(conn, p, beforeState)
          if (resourceInDb.length > 0) {
            serverNodeCrudOperator.updateProcessorResource(conn, p.copy(resources = resourceInDb.map(_.copy(status = afterState))))
          }
          if  (afterCall!=null){
            afterCall(conn,p)
          }
        })
      }catch{
        case e :Exception =>{
          e.printStackTrace()
        }
      }

  }




}
