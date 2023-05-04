package com.webank.eggroll.core.resourcemanager

import com.google.gson.Gson
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, CoreConfKeys, NodeManagerConfKeys, ProcessorStatus, ResourceManagerConfKeys, ResourceOperationStauts, ResourceOperationType, ResourceTypes, ServerNodeTypes, StringConstants}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErResourceAllocation, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.ServerNodeStatus.{HEALTHY, INIT}
import com.webank.eggroll.core.session.{RuntimeErConf, StaticErConf}
import com.webank.eggroll.core.env.{Shell, SysInfoLinux}
import com.webank.eggroll.core.util.{FileSystemUtils, GetSystemInfo, Logging, NetUtils, ProcessUtils}

import java.io.File
import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.ListBuffer



object NodeManagerMeta{
  var status=INIT
  var serverNodeId = -1:Long;
  var clusterId = -1:Long;
   def refreshServerNodeMetaIntoFile(): Unit = {
     var filePath = System.getenv("EGGROLL_HOME")+StringConstants.SLASH  +CoreConfKeys.EGGROLL_DATA_DIR.get()+ StringConstants.SLASH+"NodeManagerMeta";
     var  gson= new Gson()
      FileSystemUtils.fileWriter(filePath, gson.toJson(NodeManagerMeta))
  }
  def loadNodeManagerMetaFromFile():Unit = {
    var filePath = System.getenv("EGGROLL_HOME") + StringConstants.SLASH + CoreConfKeys.EGGROLL_DATA_DIR.get() + StringConstants.SLASH + "NodeManagerMeta";
    if(new File(filePath).exists()){
      var gson = new Gson()
      var content = FileSystemUtils.fileReader(filePath)
      var  contentMap = gson.fromJson(content,classOf[NodeManagerMeta]);
      println(s"================contentMap ============${content}  ${contentMap}")
      NodeManagerMeta.serverNodeId = contentMap.serverNodeId
      NodeManagerMeta.clusterId = contentMap.clusterId
    }
  }
}
case  class  NodeManagerMeta(status :String,
 serverNodeId : Long,
 clusterId : Long)

trait NodeManager {
  def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def stopContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def killContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def heartbeat(processor: ErProcessor): ErProcessor
  def allocateResource(erResourceAllocation: ErResourceAllocation):ErResourceAllocation
  def queryNodeResource(erServerNode: ErServerNode):ErServerNode
  def checkNodeProcess (processor: ErProcessor) : ErProcessor

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

  override def allocateResource(erResourceAllocation: ErResourceAllocation): ErResourceAllocation = {
    logInfo(s"receive allocateResource request ${erResourceAllocation}")
    var  result =erResourceAllocation.operateType match {
      case "CHECK"=>{
        var enough = true
        erResourceAllocation.resources.foreach(r=>{
          enough = enough&  NodeResourceManager.checkResourceIsEnough(r.resourceType,r.total)
        })
        var  result:ErResourceAllocation = null
        if(enough)
         erResourceAllocation.copy(status = ResourceOperationStauts.SUCCESS )
        else
          erResourceAllocation.copy(status = ResourceOperationStauts.FAILED)
      }
      case "ALLOCATE" =>{
        var success = true
        erResourceAllocation.resources.foreach(r=>{
            NodeResourceManager.allocateResource(r.resourceType,r.total)
        })
        erResourceAllocation.copy(status = ResourceOperationStauts.SUCCESS )
      }
      case "FREE" =>{
        var success = true
        erResourceAllocation.resources.foreach(r=>{
          NodeResourceManager.freeResource(r.resourceType,r.total)
        })
        erResourceAllocation.copy(status = ResourceOperationStauts.SUCCESS )
      }
    }
    logInfo(s"allocateResource result ${result}")
    return result
  }

  override def queryNodeResource(erServerNode: ErServerNode): ErServerNode = {
    NodeResourceManager.queryNodeResource(erServerNode)
  }

  override def checkNodeProcess(processor: ErProcessor): ErProcessor = {
     var  result:ErProcessor= null
     if(ProcessUtils.checkProcess(processor.pid.toString)){
       result= processor.copy(status=ProcessorStatus.RUNNING)
     }else{
       result= processor.copy(status=ProcessorStatus.KILLED)
     }
    logInfo(s"check processor pid ${processor.pid} return  ${result.status} ");
    result
  }
}

case class ResourceEvent( resourceType:String, count:Long)

object  NodeResourceManager extends  Logging {

  private  var sysInfo= if(Shell.LINUX)new  SysInfoLinux else null
  private  var client = new  ClusterManagerClient
  private  var resourceEventQueue=  new ArrayBlockingQueue[ResourceEvent](100);

  private var physicalMemorySize = getPhysicalMemorySize
  private var heartBeatThread= new HeartBeatThread()
  private var resourceCountThread = new ResourceCountThread()
  private var resourceMap = mutable.Map[String,ResourceWrapper](
      ResourceTypes.VCPU_CORE-> ResourceWrapper(resourceType=ResourceTypes.VCPU_CORE, total=
          new AtomicLong(StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_CPU_VCORES.key,getAvailableProcessors))),
       ResourceTypes.PHYSICAL_MEMORY ->  ResourceWrapper(resourceType = ResourceTypes.PHYSICAL_MEMORY, total = new AtomicLong(getPhysicalMemorySize)),
      ResourceTypes.VGPU_CORE-> ResourceWrapper(resourceType=ResourceTypes.VGPU_CORE,total= new AtomicLong(StaticErConf.getLong(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_GPU_VCORES.key,getGpuSize()))
  ));

  def getResourceWrapper(rType:String ):Option[ResourceWrapper]={
    resourceMap.get(rType)
  }
   def checkResourceIsEnough(rtype:String ,required :Long ): Boolean ={
    var resourceWrapper =  getResourceWrapper(rtype)
     logInfo(s"checkResourceIsEnough ${rtype} ${required}")
    if(resourceWrapper!=None){
      var  left = resourceWrapper.get.total.get() - resourceWrapper.get.allocated.get()
      logInfo(s"request ${required} left ${left}")
      if(required<= left)
        return true;
    }
    false
  }

  def  fireResourceChangeEvent(event:ResourceEvent):Unit={
    resourceEventQueue.put(event);
  }


  def  freeResource(rtype:String,count:Long):Long = {
    require(count>0)
    var resourceWrapper = getResourceWrapper(rtype)
    resourceWrapper.get.allocated.addAndGet(-count)
  }
  def  allocateResource(rtype:String ,count:Long):Long ={
    require(count>0)
    var resourceWrapper = getResourceWrapper(rtype)
    resourceWrapper.get.allocated.addAndGet(count);
  }


  def  start() :Unit={
    NodeManagerMeta.loadNodeManagerMetaFromFile()
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

  def getGpuSize():Long = {
    if(Shell.LINUX){
      sysInfo.getGpuNumber
    }else{
      0
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

            //logInfo(s"resource ==========${resourceMap}")
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




  def  queryNodeResource(erServerNode: ErServerNode):ErServerNode = {

     erServerNode.copy(id= NodeManagerMeta.serverNodeId,resources = resourceMap.values.toArray.filter(r=>{r.total != -1})
                    .map(r=>{ErResource(resourceType = r.resourceType,
                                      total = r.total.get(),
                      used=r.used.get(),
                      allocated = r.allocated.get())}))


//      logInfo(s"NodeManger registerResource ${param}")
//      client.registerResource(param)

  }

//  class ResourceReportThread extends Thread{
//      override   def  run(): Unit ={
//        while (true){
//          try {
//            resourceEventQueue.poll()
//            registerResource(ErServerNode(id=NodeManagerMeta.serverNodeId,clusterId = NodeManagerMeta.clusterId))
//          }catch{
//            case t: Throwable =>
//          }
//
//        }
//      }
//  }

  class HeartBeatThread extends Thread{
    override def run(){
      var  notOver : Boolean = true
      while(notOver){
        try {
          var serverNode = client.nodeHeartbeat(ErServerNode(id = NodeManagerMeta.serverNodeId,
            nodeType = ServerNodeTypes.NODE_MANAGER,
            endpoint = ErEndpoint(host = StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST, NetUtils.getLocalHost),
              port = StaticErConf.getString(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT).toInt),
            status = NodeManagerMeta.status
          ))
          logDebug(s"node heart beat return ${serverNode}")

//          logInfo(s"cluster manager return ${serverNode}")
//          logInfo(s"======node manager status ${NodeManagerMeta.status}")
          if (serverNode != null) {
          if (NodeManagerMeta.status.equals(INIT)) {
              NodeManagerMeta.status =  HEALTHY

              NodeManagerMeta.serverNodeId = serverNode.id;
              NodeManagerMeta.clusterId = serverNode.clusterId
            logInfo(s"==============================${NodeManagerMeta.serverNodeId}")
              NodeManagerMeta.refreshServerNodeMetaIntoFile()

              //   上报资源
              //registerResource(serverNode)

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
