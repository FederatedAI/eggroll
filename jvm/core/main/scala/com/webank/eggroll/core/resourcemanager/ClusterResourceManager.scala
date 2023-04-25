package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.{NodeManagerConfKeys, ResourceOperationStauts, ResourceOperationType, ResourceTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErResourceAllocation, ErServerNode}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf

import scala.collection.mutable

object ClusterResourceManager {

    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()

    def  allocateResource(sessionId :String,processors :Array[ErProcessor] ) : Unit={
      var   resourceMap = flatResources(processors)
      var  result = true
      resourceMap.par.foreach(e=>{e._1
        var node =   serverNodeCrudOperator.getServerNode(ErServerNode(id=e._1))
        val nodeManagerClient = new NodeManagerClient(
          ErEndpoint(host = node.endpoint.host,
            port = StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1)))
        var  nodeResult = nodeManagerClient.allocateResource(ErResourceAllocation(e._1,operateType = ResourceOperationType.ALLOCATE,resources = e._2));
        result &= nodeResult.status==ResourceOperationStauts.SUCCESS
      })
    }

    private def  flatResources(processors: Array[ErProcessor]): Map[Long, Array[ErResource]] ={
      processors.groupBy(p=>p.serverNodeId).mapValues(v=>{
        v.flatMap(_.resouces).groupBy(_.resourceType).mapValues(_.reduce((x,y)=>{
          x.copy(total=x.total+y.total)
        })).values.toArray
      })
    }

    def  checkResource(processors :Array[ErProcessor]): Boolean={
       var   resourceMap = flatResources(processors)

//      resourceMap.foreach((e)=>{
//        print(e._1)
//        print(" ")
//        e._2.foreach((x)=>{
//          print(x)
//        })
//      })
      var  result = true
        resourceMap.par.foreach(e=>{e._1
          var node =   serverNodeCrudOperator.getServerNode(ErServerNode(id=e._1))
            val nodeManagerClient = new NodeManagerClient(
                ErEndpoint(host = node.endpoint.host,
                    port = StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1)))
            var  nodeResult = nodeManagerClient.allocateResource(ErResourceAllocation(e._1,operateType = ResourceOperationType.CHECK,resources = e._2));
          result &= nodeResult.status==ResourceOperationStauts.SUCCESS
        })
      result
    }
    def  checkResouce():Unit={

    }

    def  freeResource(): Unit={

    }

    def  main(args: Array[String]) :Unit = {
        var   temp =  Array(ErProcessor(serverNodeId = 1,resouces = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1)))
        ,
            ErProcessor(serverNodeId = 2,resouces = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1),ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 3),ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 3)))
        )
        checkResource(temp)

    }

}
