package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, CoreConfKeys, NodeManagerConfKeys, ResourceManagerConfKeys, ResourceTypes, ServerNodeTypes, StringConstants}
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.ServerNodeStatus.{HEALTHY, INIT}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.session.{RuntimeErConf, StaticErConf}
import com.webank.eggroll.core.env.{Shell, SysInfoLinux}
import com.webank.eggroll.core.util.{GetSystemInfo, Logging, NetUtils}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

object NodeManagerMeta{
  var status=INIT
  var serverNodeId = -1:Long;
  var clusterId = -1:Long;
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

  private  var sysInfo= if(Shell.LINUX)new  SysInfoLinux else null
  private  var client = new  ClusterManagerClient

  private var physicalMemorySize = getPhysicalMemorySize
  private var heartBeatThread= new HeartBeatThread()
  private var resourceCountThread = new ResourceCountThread()
  private var resourceMap = mutable.Map[String,ResourceWrapper](
      ResourceTypes.VCPU_CORE-> ResourceWrapper(resourceType=ResourceTypes.VCPU_CORE, total=
          new AtomicLong(StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_CPU_VCORES.key,getAvailableProcessors))),
       ResourceTypes.PHYSICAL_MEMORY ->  ResourceWrapper(resourceType = ResourceTypes.PHYSICAL_MEMORY, total = new AtomicLong(getPhysicalMemorySize)),
      ResourceTypes.VGPU_CORE-> ResourceWrapper(resourceType=ResourceTypes.VGPU_CORE,total= new AtomicLong(StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_GPU_VCORES.key,0))
  ));


  def getResourceWrapper(rType:String ):Option[ResourceWrapper]={
    resourceMap.get(rType)
  }

   def checkResourceIsEnough(rtype:String ,count :Long ): Boolean ={
    var resourceWrapper =  getResourceWrapper(rtype)
    if(resourceWrapper!=None){
      var  left = resourceWrapper.get.total.get() - resourceWrapper.get.allocated.get()
      if(count>= left)
        return true;
    }
    false
  }
    def  allocateResource(rtype:String ,count:Long):Long ={
    require(count>0)
    var resourceWrapper = getResourceWrapper(rtype)
    resourceWrapper.get.allocated.addAndGet(count);
  }



  def  start() :Unit={
    heartBeatThread.start()
    resourceCountThread.start()
  }
  def getPhysicalMemorySize(): Long ={
    if(Shell.LINUX){
      sysInfo.getPhysicalMemorySize
    }else{
      GetSystemInfo.getTotalMemorySize
    }
  }
  def getAvailablePhysicalMemorySize():Long ={
    if(Shell.LINUX){
      sysInfo.getAvailablePhysicalMemorySize
    }else{
      GetSystemInfo.getFreePhysicalMemorySize
    }
  }
  def getAvailableProcessors():Int={
    if(Shell.LINUX){
      sysInfo.getNumCores
    }else{
      GetSystemInfo.getAvailableProcessors
    }
  }


  def countMemoryResource():Unit={
    if(Shell.LINUX){
      var  total = sysInfo.getPhysicalMemorySize;
      var  available = sysInfo.getAvailablePhysicalMemorySize
      resourceMap.get(ResourceTypes.PHYSICAL_MEMORY).get.used.set(physicalMemorySize-available)
    }else{
      var  available =GetSystemInfo.getFreePhysicalMemorySize
      resourceMap.get(ResourceTypes.PHYSICAL_MEMORY).get.used.set(physicalMemorySize-available)

    }
      }
  def countCpuResource():Unit = {
    if(Shell.LINUX){
      var coreUsed = sysInfo.getNumVCoresUsed;
      resourceMap.get(ResourceTypes.VCPU_CORE).get.used.set( coreUsed.toInt);
    }else {
      resourceMap.get(ResourceTypes.VCPU_CORE).get.used.set(GetSystemInfo.getProcessCpuLoad.toInt);
    }
  }

  def countGpuResource():ErResource = {
    null
  }


  class ResourceCountThread  extends  Thread{
      override  def  run(): Unit = {
        while(true){
          try{
            countCpuResource();
            countMemoryResource();
            countGpuResource();

            logInfo(s"resource ==========${resourceMap}")
          }
          catch {
            case t: Throwable =>
              t.printStackTrace()
              logError("register node error ")
          }
          Thread.sleep(10000)
        }
      }
  }

  def  registerResource(erServerNode: ErServerNode):ErServerNode = {
      var  param = erServerNode.copy(id= NodeManagerMeta.serverNodeId,resources = resourceMap.values.toArray.filter(r=>{r.total != -1})
                    .map(r=>{ErResource(resourceType = r.resourceType,total = r.total.get())}))
      logInfo(s"NodeManger registerResource ${param}")
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
              NodeManagerMeta.clusterId = serverNode.clusterId
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
        Thread.sleep(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt)
      }
    }
  }
}
