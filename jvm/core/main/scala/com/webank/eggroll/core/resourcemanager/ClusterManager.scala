package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.{ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.meta.{ErProcessor, ErResource, ErServerNode}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator

import java.util.concurrent.ConcurrentHashMap
import com.webank.eggroll.core.util.{Logging, ToStringUtils}


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ClusterManager {
        def registerResource(data: ErServerNode): ErServerNode
        def nodeHeartbeat(data : ErServerNode): ErServerNode
        }

        object ClusterManagerService{

        def  start(){}

        var  NodeHeartbeatChecker =  new Thread(()=>{
             var nodes = ServerNodeCrudOperator.doGetServerNodes(ErServerNode())
        });
        }

        object ClusterResourceManager{
        var cpuResourceMap =  new ConcurrentHashMap[Long, ResourceWrapper]()
        var gpuResourceMap =  new ConcurrentHashMap[Long,ResourceWrapper]();
        var memoryResourceMap = new ConcurrentHashMap[Long,ResourceWrapper]();
        }
        case class  ResourceWrapper(var resourceType:String,var totol:Long,var used : Long)

class ClusterManagerService extends   ClusterManager with Logging{

    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()

    override def registerResource(data: ErServerNode):ErServerNode = {
        var existResources = serverNodeCrudOperator.getNodeResources(data.id,"")
        var  registedResources = data.resources;
        var  updateResources = ArrayBuffer[ErResource]()
        var  deleteResources = ArrayBuffer[ErResource]()
        var  insertResources = ArrayBuffer[ErResource]()
        /**
         * 删掉多余资源，插入新增资源，修改已有资源
         */
        existResources.foreach(e=>{
                var  needUpdate = registedResources.filter(r=>{r.resourceType==e.resourceType})
        if(needUpdate.length>0){
            updateResources++=needUpdate
        }else{
            deleteResources+=e
        }
    })
        registedResources.foreach(r=>{
        if(!updateResources.contains(r)){
            insertResources+=r
        }
    })
        ServerNodeCrudOperator.registerResource(data.id,insertResources.toArray,updateResources.toArray,deleteResources.toArray)
        data
    }

    override def nodeHeartbeat(data: ErServerNode): ErServerNode = {
        logInfo(s" ${data}")
        ServerNodeCrudOperator.doCreateOrUpdateServerNode(input = data, true )
    }
}


