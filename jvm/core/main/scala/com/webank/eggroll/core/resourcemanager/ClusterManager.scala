package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT
import com.webank.eggroll.core.constant.NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL
import com.webank.eggroll.core.constant.{ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.meta.{ErProcessor, ErResource, ErServerNode}
import com.webank.eggroll.core.resourcemanager.ClusterManagerService.nodeHeartbeatChecker
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator

import java.util.concurrent.ConcurrentHashMap
import com.webank.eggroll.core.util.{Logging, ToStringUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ClusterManager {
        def registerResource(data: ErServerNode): ErServerNode
        def nodeHeartbeat(data : ErServerNode): ErServerNode
        }
//        object ClusterResourceRegister{
//        var cpuResourceMap =  new ConcurrentHashMap[Long, ResourceWrapper]()
//        var gpuResourceMap =  new ConcurrentHashMap[Long,ResourceWrapper]();
//        var memoryResourceMap = new ConcurrentHashMap[Long,ResourceWrapper]();
//        }
 

object ClusterManagerService extends Logging {
  val  nodeHeartbeatChecker =  new Thread(()=>{
    var expire = CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT.get().toInt*CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt
    while(true){
      try {
        var  now  = System.currentTimeMillis();
        var nodes = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(status = ServerNodeStatus.HEALTHY))
        nodes.foreach(n=>{
          var interval = now - (if(n.lastHeartBeat != null) n.lastHeartBeat.getTime else 0)
         // logInfo(s"interval : ${n.lastHeartBeat} ${interval}  ${now} ${if(n.lastHeartBeat!=null)n.lastHeartBeat.getTime}")
           if(  interval>expire){
              logInfo(s"server node ${n} change status to LOSS")
              ServerNodeCrudOperator.doUpdateServerNode(n.copy(status = ServerNodeStatus.LOSS))
           }
        })
      }catch {
        case e: Exception =>
          e.printStackTrace()
      }
      Thread.sleep(expire+1000)
    }
  });

  def  start():Unit={
    nodeHeartbeatChecker.start()
  }





}
class ClusterManagerService extends   ClusterManager with Logging{

    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()




    override def registerResource(data: ErServerNode):ErServerNode = {
        logInfo(s"==========registerResource ${data}")
        var existResources = serverNodeCrudOperator.getNodeResources(data.id,"")
        var  registedResources = data.resources;
        var  updateResources = ArrayBuffer[ErResource]()
        var  deleteResources = ArrayBuffer[ErResource]()
        var  insertResources = ArrayBuffer[ErResource]()
        /**
         * 删掉多余资源，插入新增资源，修改已有资源
         */
        existResources.foreach(e=>{
                var  needUpdate = registedResources.filter(r=>{r.resourceType==e.resourceType}).map(_.copy(allocated = -1))
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
        logInfo(s" nodeHeartbeat ${data}")

      var result : ErServerNode=null
      val existing = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(id = data.id,clusterId = data.clusterId,endpoint = data.endpoint))
      if (existing.nonEmpty) {
        var existNode = existing(0)
        var param = data.copy(id = existNode.id)
        ServerNodeCrudOperator.doUpdateServerNode(param, true)
        if(ServerNodeCrudOperator.doQueryNodeResources(data.id).length==0){
          var nodeManagerClient = new NodeManagerClient(data.endpoint)
          var node = nodeManagerClient.queryNodeResource(param)
        //  logInfo(s"1111111111111111111111111111 ${node}");
          result= registerResource(node.copy(id = existNode.id))
        }else{
          result= param
        }
      } else {
       var  resultErServerNode=  ServerNodeCrudOperator.doCreateServerNode(data)
        var  nodeManagerClient = new  NodeManagerClient(data.endpoint)
        var  node = nodeManagerClient.queryNodeResource(resultErServerNode)
      //  logInfo(s"22222222222222222222222 ${node}");
        result = registerResource(node.copy(id=resultErServerNode.id))
      }
    //  logInfo(s"=======result ${result}");
     result

//        ServerNodeCrudOperator.doCreateOrUpdateServerNode(input = data, true )
//
//
//        var nodeResouces = ServerNodeCrudOperator.doQueryNodeResources(data.id)
//        if(nodeResouces.length==0){
//
//        }


    }


}

