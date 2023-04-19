package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ResourceManagerConfKeys
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.ServerNodeStatus.{HEALTHY, INIT}
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, CoreConfKeys, NodeManagerConfKeys, ResourceManagerConfKeys, ResourceTypes, ServerNodeTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.session.{RuntimeErConf, StaticErConf}
import com.webank.eggroll.core.util.{Logging, NetUtils}

import scala.collection.mutable

object NodeManagerMeta{
  var status=INIT
  var serverNodeId = -1:Long;
  var clusterId = "";
}

trait NodeManager {
  def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def stopContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def killContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def heartbeat(processor: ErProcessor): ErProcessor
}

class NodeManagerService extends NodeManager with Logging {

  var status =  HEALTHY

  var host = StaticErConf.getString(
    ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST)
  var port  = StaticErConf.getString(
    ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT)


  var  client = new  ClusterManagerClient()

  override def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    operateContainers(sessionMeta, "start")
  }

  override def stopContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    operateContainers(sessionMeta, "stop")
  }

  override def killContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    operateContainers(sessionMeta, "kill")
  }

  private def operateContainers(sessionMeta: ErSessionMeta, opType: String): ErSessionMeta = {
    val processorPlan = sessionMeta.processors

    val runtimeConf = new RuntimeErConf(sessionMeta)
    val myServerNodeId = runtimeConf.getLong(ResourceManagerConfKeys.SERVER_NODE_ID, -1)

    val result = processorPlan.par.map(p => {
      if (p.serverNodeId == myServerNodeId) {
        val container = new Container(runtimeConf, p.processorType, p.id)
        opType match {
          case "start" => container.start()
          case "stop" => container.stop()
          case "kill" => container.kill()
          case _ => throw new IllegalArgumentException(s"op not supported: '${opType}'")
        }
      }
    })

    sessionMeta
  }

  override def heartbeat(processor: ErProcessor): ErProcessor = {
    logInfo(s"nodeManager receive heartbeat ${processor}")
    client.heartbeat(processor);
  }
}



object  NodeResourceManager extends  Logging {

  private  var client = new  ClusterManagerClient()
  //StaticErConf.getString(CoreConfKeys.CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE, SerdesTypes.PROTOBUF)
  private  var resourceMap = mutable.Map(ResourceTypes.CPU->ErResource(resourceType=ResourceTypes.CPU,
    total=StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_CPU_VCORES.key,-1)),
    ResourceTypes.MEMORY->ErResource(resourceType=ResourceTypes.MEMORY),
    ResourceTypes.GPU->ErResource(resourceType=ResourceTypes.GPU,total=StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_GPU_VCORES.key,-1))
  )




  def  start() :Unit={
    val heartBeatThread = new HeartBeatThread()
    heartBeatThread.start()
  }

  def countMemoryResource():ErResource={
    null
  }
  def countCpuResource():ErResource = {
    null
  }

  def countGpuResource():ErResource = {
    null
  }


  class ResourceCountThread  extends  Thread{
      override  def  run(): Unit = {

      }

  }

  def  registerResource(erServerNode: ErServerNode):ErServerNode = {

      var  param = erServerNode.copy(id= NodeManagerMeta.serverNodeId,resources = resourceMap.values.toArray)

      client.registerResource(param)

  }


  class HeartBeatThread extends Thread{
    override def run(){
      var  notOver : Boolean = true
      while(notOver){
        try {

          var serverNode = client.nodeHeartbeat(ErServerNode(id = StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_ID, -1),
            nodeType = ServerNodeTypes.NODE_MANAGER,
            endpoint = ErEndpoint(host = StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST, NetUtils.getLocalHost),
              port = StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT).toInt),
            status = NodeManagerMeta.status
          ))

          logInfo(s"cluster manager return ${serverNode}")
          logInfo(s"======node manager status ${NodeManagerMeta.status}")
          if (serverNode != null) {
          if (NodeManagerMeta.status.equals(INIT)) {
              NodeManagerMeta.status =  HEALTHY;
              NodeManagerMeta.serverNodeId = serverNode.id;
              //   上报资源
              registerResource(serverNode)

          }
        }
//          if (serverNode != null) {
//            notOver = false
//          }
        }catch {
          case t: Throwable =>
            t.printStackTrace()
            logError("register node error ")
        }
        Thread.sleep(StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL,10000))
      }
    }
  }
}
