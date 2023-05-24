package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.{ResourceStatus, ResourceTypes, StringConstants}
import com.webank.eggroll.core.meta.{ErProcessor, ErResource}
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.{logInfo, serverNodeCrudOperator}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import scala.collection.mutable.ArrayBuffer

object ResourceStateMachine extends Logging{

  def  changeState(connection: Connection,processors: Array[ErProcessor],beforeState:String,afterState:String ): Unit =synchronized{
          var  stateLine =  beforeState+"_"+afterState

        logInfo(s"=======resource====stateLine==========${stateLine}")

        stateLine match {
          case "init_pre_allocated" =>  preAllocateResource(connection,processors)
          case "pre_allocated_allocated" =>  updateResource(connection,processors,beforeState,afterState,afterCall = (conn,p)=>{
                countAndUpdateNodeResource(conn,p.serverNodeId)

          })
          case "pre_allocated_allocate_failed" => updateResource(connection,processors,beforeState,afterState,afterCall = (conn,p)=>{
              countAndUpdateNodeResource(conn,p.serverNodeId)
          })
          case "allocated_return"=> updateResource(connection,processors,beforeState,afterState,beforeCall=(conn,p)=>{
            var allocatedCount =  serverNodeCrudOperator.countNodeResource(conn,p.serverNodeId)
            logInfo(s"before======allocatedCount===========${allocatedCount.mkString}")
          },
            afterCall = (conn,p)=>{
              countAndUpdateNodeResource(conn,p.serverNodeId)

          })
          case _ => println("=================resource======error====status======="+stateLine)


        }
  }

  private def  countAndUpdateNodeResource(conn: Connection,serverNodeId: Long): Unit ={
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
            case ResourceStatus.PRE_ALLOCATED => preAllocated += r.allocated
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

    logInfo(s"updateNodeResource  ======== nodeId ${serverNodeId}======  ${paramResources.toArray.mkString}")
    serverNodeCrudOperator.updateNodeResource(conn, serverNodeId, paramResources.toArray)

  }



  private def preAllocateResource(conn : Connection,processors: Array[ErProcessor]): Unit = synchronized {

    //ServerNodeCrudOperator.dbc.withTransaction(conn => {
      serverNodeCrudOperator.insertProcessorResource(conn, processors);
    processors.groupBy(_.serverNodeId).foreach(e=>{
      countAndUpdateNodeResource(conn,e._1)
    }
  )



  }


  private def  updateResource(conn: Connection,
                      processors: Array[ErProcessor],
                      beforeState:String,
                      afterState:String,
                      beforeCall:(Connection,ErProcessor)=>Unit =null,
                      afterCall:(Connection,ErProcessor)=>Unit =null):  Unit=synchronized{
   // logInfo(s"return resource ${processors.mkString("<",",",">")}")
  //  ServerNodeCrudOperator.dbc.withTransaction(conn=> {
      try {

        processors.foreach(p => {

          if (beforeCall != null) {
            beforeCall(conn,p)
          }
          var resourceInDb = serverNodeCrudOperator.queryProcessorResource(conn, p, beforeState)
//          resourceInDb.foreach(r => {
//            logInfo(s"processor ${p.id} return resource ${r}")
//          })
          if (resourceInDb.length > 0) {
            serverNodeCrudOperator.updateProcessorResource(conn, p.copy(resources = resourceInDb.map(_.copy(status = afterState))))
//            serverNodeCrudOperator.countNodeResource(conn,p.serverNodeId,beforeState)
//            serverNodeCrudOperator.countNodeResource(conn,p.serverNodeId,afterState)

 //           serverNodeCrudOperator.updateNodeResource(conn, p.serverNodeId, resourceInDb)
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
   // }
 //   )
  }




}
