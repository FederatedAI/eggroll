package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT
import com.webank.eggroll.core.constant.NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL
import com.webank.eggroll.core.constant.ProcessorEventType.PROCESSOR_LOSS
import com.webank.eggroll.core.constant.{ProcessorStatus, ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.meta.{ErNodeHeartbeat, ErProcessor, ErResource, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.ClusterManagerService.{createNewNode, nodeHeartbeatChecker, nodeHeartbeatMap, updateNode}
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.serverNodeCrudOperator
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator

import java.util.concurrent.ConcurrentHashMap
import com.webank.eggroll.core.util.{Logging, ToStringUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ClusterManager {
      //  def registerResource(data: ErServerNode): ErServerNode
        def nodeHeartbeat(data : ErNodeHeartbeat): ErNodeHeartbeat
        }

object ClusterManagerService extends Logging {
  var  processorEventCallbackRegister = mutable.Map[String,ProcessorEventCallback]()
  var  nodeHeartbeatMap = mutable.Map[Long,ErNodeHeartbeat]()
  lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()
  def  registerProcessorCallback(processType:String,sessionEventCallback: ProcessorEventCallback):Unit={
    processorEventCallbackRegister.put(processType,sessionEventCallback)
  }
  var  nodeProcessChecker = new  Thread(()=>{
    var  serverNodeCrudOperator = new ServerNodeCrudOperator
    while(true) {
      try {
        var now = System.currentTimeMillis();
        var erProcessors = serverNodeCrudOperator.queryProcessor(null,ErProcessor(status = ProcessorStatus.RUNNING));
        var  grouped =erProcessors.groupBy(x=>{
          x.serverNodeId})
        grouped .par.
          foreach(e => {
            var serverNode = serverNodeCrudOperator.getServerNode(ErServerNode(id = e._1))
            var nodeManagerClient = new NodeManagerClient(serverNode.endpoint)
            e._2.par.foreach(processor => {
              var result = nodeManagerClient.checkNodeProcess(processor)
              logDebug(s"check ${processor.id} node process ${processor.pid} return ${result.status}")
              if (result.status == ProcessorStatus.KILLED) {
                var processors = Array(processor.copy(status = ProcessorStatus.KILLED))
                ClusterResourceManager.returnResource(processors, (conn) => {
                  serverNodeCrudOperator.updateNodeProcessor(conn, processors)
                  var activeCount = serverNodeCrudOperator.queryProcessor(conn,processor.copy(status = ProcessorStatus.RUNNING)).length
                  serverNodeCrudOperator.updateSessionProcessCount(connection = conn, ErSessionMeta(id = processor.sessionId, activeProcCount = activeCount))
                })
                processorEventCallbackRegister.get(processor.processorType).getOrElse(new ProcessorEventCallback{
                  override def callback(event: ProcessorEvent): Unit = {
                    logInfo(s"processor type ${processor.processorType} can not find callback")
                  }
                }).callback(ProcessorEvent(eventType = PROCESSOR_LOSS,erProcessor=processor))

              }
            })
          })
    }
    catch
    {
      case e: Exception =>
        e.printStackTrace()
    }
      Thread.sleep(CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt)
    }
  }
    )


  val  nodeHeartbeatChecker =  new Thread(()=>{
    var expire = CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT.get().toInt*CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt
    while(true){
      try {
        //logInfo("node heart beat checker running")
        var  now  = System.currentTimeMillis();
        var nodes = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(status = ServerNodeStatus.HEALTHY))
        nodes.foreach(n=>{
          var interval = now - (if(n.lastHeartBeat != null) n.lastHeartBeat.getTime else 0)
         // logInfo(s"interval : ${n.lastHeartBeat} ${interval}  ${now} ${if(n.lastHeartBeat!=null)n.lastHeartBeat.getTime}")
           if(  interval>expire){
              logInfo(s"server node ${n} change status to LOSS")
              updateNode(n.copy(status = ServerNodeStatus.LOSS),false,false)
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
    nodeProcessChecker.start()
  }

  def updateNode(serverNode : ErServerNode, needUpdateResource: Boolean,isHeartbeat :Boolean):ErServerNode=synchronized{
    serverNodeCrudOperator.updateServerNodeById(serverNode,isHeartbeat)
    if(needUpdateResource)
        registerResource(serverNode)
    serverNode
  }

   def registerResource(data: ErServerNode):ErServerNode = synchronized{
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

  def createNewNode(serverNode : ErServerNode):ErServerNode=synchronized{
    var  existNode =  serverNodeCrudOperator.getServerNode(serverNode.copy(status = ""))
    if(existNode!=null){
      existNode =serverNodeCrudOperator.createServerNode(serverNode);
    }
    registerResource(serverNode.copy(id=existNode.id))
    existNode
  }
}
class ClusterManagerService extends   ClusterManager with Logging{

    override def nodeHeartbeat(nodeHeartbeat: ErNodeHeartbeat): ErNodeHeartbeat = {
        logInfo(s" nodeHeartbeat ${nodeHeartbeat}")
      var  serverNode = nodeHeartbeat.node
      if(serverNode.id == -1){
        serverNode = createNewNode(serverNode)
      }else{
        if(nodeHeartbeatMap.contains(serverNode.id)&&(nodeHeartbeatMap(serverNode.id).id < nodeHeartbeat.id)){
          updateNode(serverNode,false,true)
        }else{
          updateNode(serverNode,true,true)
        }
      }
      nodeHeartbeatMap.put(serverNode.id,nodeHeartbeat);
      nodeHeartbeat.copy(node = serverNode)
    }


}

