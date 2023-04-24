package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.{NodeManagerConfKeys, ResourceOperationType, ResourceTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErResourceAllocation, ErServerNode}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf

import scala.collection.mutable

object ClusterResourceManager {

    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()

    def  allocateResource(sessionId :String ) : Unit={

    }

    def  checkResource(processors :Array[ErProcessor]): Unit={
       var   resourceMap = processors.groupBy(p=>p.serverNodeId).mapValues(v=>{
            v.flatMap(_.resouces)
        })
        resourceMap.par.foreach(e=>{e._1
          var node =   serverNodeCrudOperator.getServerNode(ErServerNode(id=e._1))
            val nodeManagerClient = new NodeManagerClient(
                ErEndpoint(host = node.endpoint.host,
                    port = StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1)))
            nodeManagerClient.allocateResource(ErResourceAllocation(e._1,operateType = ResourceOperationType.CHECK,resources = e._2));
        })
    }
    def  checkResouce():Unit={

    }

    def  freeResource(): Unit={

    }

    def  main(args: Array[String]) :Unit = {
        var   temp =  Array(ErProcessor(serverNodeId = 1,resouces = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1)))
        ,
            ErProcessor(serverNodeId = 2,resouces = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1)))
        )
        checkResource(temp)

    }

}
