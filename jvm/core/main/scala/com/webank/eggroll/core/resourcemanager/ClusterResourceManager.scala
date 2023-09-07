package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.SessionConfKeys.{EGGROLL_RESOURCE_COUNT_INTERVAL, EGGROLL_RESOURCE_DISPATCH_INTERVAL, EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.containers.JobProcessorTypes
import com.webank.eggroll.core.datastructure.FifoBroker
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.SessionManagerService.smDao
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object ClusterResourceManager extends Logging{

//    var nodeMaxResourceMap: mutable.Map[Long,mutable.Map[String,Long]] = mutable.Map[Long,mutable.Map[String,Long]]()
    var clusterMaxResourceMap:mutable.Map[String,Long] = mutable.Map[String,Long]()

    def  start():Unit={
      dispatchThread.start()
      lockCleanThread.start()
      maxResourceCountThread.start()
    }

    def checkMaxResource(resourceType :String,count :Long): Boolean ={
      var  max:Long =clusterMaxResourceMap.getOrElse(resourceType,0)
      var result =if( count <= max) true else false
      logInfo(s"check max resource in cluster ,request type ${resourceType} count ${count} max ${max} result ${result}")
      result
    }


    def countMaxResource(serverNodes:Array[ErServerNode]): Unit ={

      var newClusterMaxMap:mutable.Map[String,Long] = mutable.Map[String,Long]()
      serverNodes.foreach(n=>{
        n.resources.foreach(r=>{
          var  gloablTotal :Long = newClusterMaxMap.getOrElse(r.resourceType,0)
          var  total:Long = r.total+gloablTotal
          newClusterMaxMap(r.resourceType)=total
        })
      })
      this.clusterMaxResourceMap= newClusterMaxMap;
//      logInfo(s"cluster total resource ${clusterMaxResourceMap}")
    }

    var   sessionLockMap = new  ConcurrentHashMap[String,ReentrantLock]()

    var   killJobMap = new ConcurrentHashMap[String,Long]()
    val   applicationQueue   =   new FifoBroker[ResourceApplication]
    var   resourceEventQueue = new FifoBroker[ResourceEvent]
    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()
    private lazy val smDao = new SessionMetaDao
    var  dispatchThread = new  Thread(()=>{
      logInfo("resource dispatch thread start !!!")
      while(true){
        var resourceApplication:ResourceApplication =null
        if(applicationQueue.broker.size()>0) {
          resourceApplication = applicationQueue.broker.peek()
          logInfo(s"resource application queue size ${applicationQueue.broker.size()}")
        }
        else {
          Thread.sleep(EGGROLL_RESOURCE_DISPATCH_INTERVAL.get().toInt)
        }
        try{
          breakable {
            if(resourceApplication!=null) {
              var now = System.currentTimeMillis()
              var serverNodes  :Array[ErServerNode]= null
              try {
                lockSession(resourceApplication.sessionId)
                if(killJobMap.contains(resourceApplication.sessionId)){
                  logError(s"session ${resourceApplication.sessionId} is already canceled , drop it")

                  applicationQueue.broker.remove()
                  break()
                }

                if (resourceApplication.waitingCount.get() == 0
                ) {
                  //过期资源申请
                  logError(s"expired resource request : ${resourceApplication} !!!")
                  applicationQueue.broker.remove()
                  break()
                }

                var tryCount: Int =0
                do{
                  serverNodes = getServerNodeWithResource();
                  tryCount+=1
                  if(serverNodes==null||serverNodes.length==0)
                      Thread.sleep(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toLong)
                }while((serverNodes==null||serverNodes.length==0)&&tryCount<2)
                var enough = checkResourceEnough(serverNodes, resourceApplication)
                logInfo(s"check resource is enough ? ${enough}")
                if (!enough) {
                  resourceApplication.resourceExhaustedStrategy match {
                    case ResourceExhaustedStrategy.IGNORE => ;

                    case ResourceExhaustedStrategy.WAITING =>
                      Thread.sleep(EGGROLL_RESOURCE_DISPATCH_INTERVAL.get().toInt)
                      logInfo("resource is not enough , waiting next loop")
                      break()
                    case ResourceExhaustedStrategy.THROW_ERROR =>
                      resourceApplication.status.set(1)
                      resourceApplication.countDown()
                      applicationQueue.broker.remove()
                      break()
                  }
                }
                resourceApplication.dispatchStrategy match {
                  case DispatchStrategy.REMAIN_MOST_FIRST => remainMostFirstDispatch(serverNodes, resourceApplication);
                  case DispatchStrategy.RANDOM => randomDispatch(serverNodes, resourceApplication);
                  case DispatchStrategy.FIX => fixDispatch(serverNodes,resourceApplication);
                  case DispatchStrategy.SINGLE_NODE_FIRST=> singleNodeFirstDispatch(serverNodes,resourceApplication)
                }

              var dispatchedProcessors = resourceApplication.resourceDispatch


                smDao.registerWithResource(ErSessionMeta(
                  id = resourceApplication.sessionId,
                  name = resourceApplication.sessionName,
                  processors = dispatchedProcessors.toArray.map(_._1),
                  totalProcCount = dispatchedProcessors.length,
                  status = SessionStatus.NEW)
                )
              }finally {
                unlockSession(resourceApplication.sessionId)
              }
              val registeredSessionMeta = smDao.getSession(resourceApplication.sessionId)

              var serverNodeMap = serverNodes.groupBy(_.id).mapValues(_.apply(0))
              var  result = registeredSessionMeta.processors.map(p=>{(p,serverNodeMap.get(p.serverNodeId).get)})
              resourceApplication.resourceDispatch.clear()
              resourceApplication.resourceDispatch.appendAll(result)
              resourceApplication.countDown()
              applicationQueue.broker.remove()
            }
          }
        }catch {
          case  e:Throwable => {
            logError("dispatch resource error: "+e.getMessage)
            e.printStackTrace()
          }
        }

      }
      logError("!!!!!!!!!!!!!!!!!!!resource dispatch thread quit!!!!!!!!!!!!!!!!")

    },"RESOURCE_DISPATCH_THREAD")


  var  maxResourceCountThread =  new  Thread(()=> {
    while (true) {
      try{

        var serverNodes = getServerNodeWithResource();
        countMaxResource(serverNodes:Array[ErServerNode])
        serverNodes.map(   node=>{
          System.currentTimeMillis();
          ResourceStateMachine.countAndUpdateNodeResource(node.id)}     )
      }catch {
        case  e:Throwable => {
          logError("count resource error: "+e.getMessage)
          e.printStackTrace()
        }
      }
      Thread.sleep(EGGROLL_RESOURCE_COUNT_INTERVAL.get().toInt)
    }
  },"SYSTEM-RESOURCE-COUNT-THREAD")





  var  lockCleanThread =  new  Thread(()=> {
    while (true) {
      logInfo("lock clean thread , prepare to run")
      var  now = System.currentTimeMillis()
      sessionLockMap.forEach((k, v) => {
        try {
          var es:ErSessionMeta  =   smDao.getSessionMain(k)
          if(es.updateTime!=null){
            var updateTime = es.updateTime.getTime
            if(now -updateTime>EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL.get().toInt&& (es.status== SessionStatus.KILLED||
              es.status==SessionStatus.ERROR||
              es.status== SessionStatus.CLOSED||
              es.status== SessionStatus.FINISHED)){
              sessionLockMap.remove(es.id)
              killJobMap.remove(es.id)
            }
          }

        }catch {
          case e:Throwable  =>
              logError(s"lock clean error ${e.getMessage}")
           // e.printStackTrace()
        }
      })
      Thread.sleep(EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL.get().toInt)
    }
  },"LOCK-CLEAN-THREAD")




  def  lockSession(sessionId:String): Unit={
     var lock =  sessionLockMap.get(sessionId)
     if(lock==null){
        sessionLockMap.putIfAbsent(sessionId,new  ReentrantLock())
        lock =  sessionLockMap.get(sessionId)
     }
    logInfo(s"lock session ${sessionId}")
    lock.lock()
  }
  def  unlockSession(sessionId:String): Unit={
    var lock =  sessionLockMap.get(sessionId)
    if(lock!=null){
      logInfo(s"unlock session ${sessionId}")
      lock.unlock()
    }
  }

   private def fixDispatch(serverNodes:Array[ErServerNode] ,resourceApplication: ResourceApplication):ResourceApplication={
     val eggsPerNode = resourceApplication.options.getOrElse(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, "1")).toInt
     var resourceType= resourceApplication.options.getOrElse("resourceType",ResourceTypes.VCPU_CORE)
     val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
     for (elem <- resourceApplication.processorTypes) {
       serverNodes.flatMap(n => (0 until eggsPerNode).map(_ => {

         var requiredProcessor = ErProcessor(
           serverNodeId = n.id,
           processorType = elem,
           commandEndpoint = ErEndpoint(n.endpoint.host, 0),
           status = ProcessorStatus.NEW,
           resources = Array(ErResource(resourceType = resourceType, allocated = 1, status = ResourceStatus.PRE_ALLOCATED)))
         if (nodeToProcessors.contains(n)) {
           nodeToProcessors(n) :+= requiredProcessor
         } else {
           nodeToProcessors(n) = Seq(requiredProcessor)
         }

       }))
     }
     var result = nodeToProcessors.flatMap { case (node, processors) =>
       processors.map(p => (p, node))
     }(collection.breakOut)
     resourceApplication.resourceDispatch.appendAll(result)
     resourceApplication
   }

   private def  randomDispatch(serverNodes:Array[ErServerNode] ,resourceApplication: ResourceApplication): ResourceApplication ={

     var requiredProcessors = resourceApplication.processors;
     val shuffledNodes = Random.shuffle(serverNodes.toSeq)
     val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
     for (index <- 0 until requiredProcessors.length) {
       var requiredProcessor = requiredProcessors(index)
       var node = shuffledNodes.head
       val host = node.endpoint.host
       val globalRank = index
       val localRank = nodeToProcessors.getOrElse(node, Seq()).size
       requiredProcessor =requiredProcessor.copy(serverNodeId = node.id,commandEndpoint = ErEndpoint(host, 0))

       if (nodeToProcessors.contains(node)) {
         nodeToProcessors(node) :+= requiredProcessor
       } else {
         nodeToProcessors(node) = Seq(requiredProcessor)
       }
     }
     var result = nodeToProcessors.flatMap { case (node, processors) =>
       processors.map(p => (p, node))
     }(collection.breakOut)
     resourceApplication.resourceDispatch.appendAll(result)
     resourceApplication
   }

  private def getServerNodeWithResource():Array[ErServerNode]={
    serverNodeCrudOperator.getServerNodesWithResource(
      ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER))
  }

  private def getNextGpuIndex(size:Int,alreadyAllocated :Array[String]): Int ={
    var  result:Int = -1

    breakable {
      for (index <- 0 until size) {
        if (!alreadyAllocated.contains(index.toString)) {
          result = index
          break()
        }
      }
    }
    logInfo(s"get next gpu index , size ${size}  alreadyAllocated ${alreadyAllocated.mkString} return ${result}")
    result
  }

  private def  singleNodeFirstDispatch(serverNodes:Array[ErServerNode],resourceApplication: ResourceApplication): ResourceApplication = {
    var requiredProcessors = resourceApplication.processors;
    var nodeResourceTupes = serverNodes.map(n => (n, n.resources.filter(_.resourceType == resourceApplication.sortByResourceType).map(_.getUnAllocatedResource).apply(0)))
      .sortWith(_._2 > _._2).toBuffer

    //    // FIXME: evenly distribute processors to nodes for now
    val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
    var nodeList = ArrayBuffer[ErServerNode]()
    nodeResourceTupes.foreach(t=>{
      for (index <- 0 until t._2.toInt)
        nodeList.append(t._1)
    })


    for (index <- 0 until requiredProcessors.length) {

      var requiredProcessor = requiredProcessors(index)
      var node = nodeList.apply(index)
      //gpu 需要编号
      var  nextGpuIndex:Int = -1
      var  newResources :ArrayBuffer[ErResource] = new  ArrayBuffer[ErResource]()
      requiredProcessor.resources.foreach(r=>{
        var  changedResource : ErResource = r
        if(r.resourceType==ResourceTypes.VGPU_CORE){
          var  gpuResourcesInNodeArray =  node.resources.filter(_.resourceType==ResourceTypes.VGPU_CORE)
          if(gpuResourcesInNodeArray.length>0){
            var gpuResourcesInNode = gpuResourcesInNodeArray.apply(0)
            gpuResourcesInNode.extentionCache.appendAll(if(gpuResourcesInNode.extention!=null) gpuResourcesInNode.extention.split(",")else Array(""))
            nextGpuIndex = getNextGpuIndex(gpuResourcesInNode.total.toInt,gpuResourcesInNode.extentionCache.toArray)
            gpuResourcesInNode.extentionCache.append(nextGpuIndex.toString)
            changedResource = changedResource.copy(extention = nextGpuIndex.toString)
          }
        }
        newResources.append(changedResource)
      })
      val host = node.endpoint.host
      requiredProcessor = requiredProcessor.copy(serverNodeId = node.id,commandEndpoint = ErEndpoint(host, 0),resources=newResources.toArray,
        options = Map(
          "cudaVisibleDevices" -> nextGpuIndex.toString
        ).asJava)

      if (nodeToProcessors.contains(node)) {
        nodeToProcessors(node) :+= requiredProcessor
      } else {
        nodeToProcessors(node) = Seq(requiredProcessor)
      }
    }
    var result = nodeToProcessors.flatMap { case (node, processors) =>
      processors.map(p => (p, node))
    }(collection.breakOut)

    logInfo(s"dispatch result ${result}")
    resourceApplication.resourceDispatch.appendAll(result)
    resourceApplication




  }


  private def  remainMostFirstDispatch(serverNodes:Array[ErServerNode],resourceApplication: ResourceApplication): ResourceApplication = {

     var requiredProcessors = resourceApplication.processors;
     var nodeResourceTupes = serverNodes.map(n => (n, n.resources.filter(_.resourceType == resourceApplication.sortByResourceType).map(_.getUnAllocatedResource).apply(0)))
       .sortWith(_._2 > _._2).toBuffer
    var allocatedGpuIndex :ArrayBuffer[String]= ArrayBuffer()
     //    // FIXME: evenly distribute processors to nodes for now
     val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
     //
     for (index <- 0 until requiredProcessors.length) {

       var requiredProcessor = requiredProcessors(index)

       var nodeTupe = nodeResourceTupes.head
       var node = nodeTupe._1

       //gpu 需要编号
       var  nextGpuIndex:Int = -1
       var  newResources :ArrayBuffer[ErResource] = new  ArrayBuffer[ErResource]()
       requiredProcessor.resources.foreach(r=>{
         var  changedResource : ErResource = r
          if(r.resourceType==ResourceTypes.VGPU_CORE){
            var  gpuResourcesInNodeArray =  node.resources.filter(_.resourceType==ResourceTypes.VGPU_CORE)
            if(gpuResourcesInNodeArray.length>0){
              var gpuResourcesInNode = gpuResourcesInNodeArray.apply(0)

              gpuResourcesInNode.extentionCache.appendAll(if(gpuResourcesInNode.extention!=null) gpuResourcesInNode.extention.split(",")else Array(""))
              nextGpuIndex = getNextGpuIndex(gpuResourcesInNode.total.toInt,gpuResourcesInNode.extentionCache.toArray)
              gpuResourcesInNode.extentionCache.append(nextGpuIndex.toString)
              changedResource = changedResource.copy(extention = nextGpuIndex.toString)
            }
          }
         newResources.append(changedResource)
       })



       nodeResourceTupes = (nodeResourceTupes.tail += nodeTupe.copy(_2 = nodeTupe._2 - 1)).sortWith(_._2 > _._2)
       val host = node.endpoint.host
       val globalRank = index
       val localRank = nodeToProcessors.getOrElse(node, Seq()).size

       requiredProcessor = requiredProcessor.copy(serverNodeId = node.id,commandEndpoint = ErEndpoint(host, 0),resources=newResources.toArray,
                     options = Map(
                       //                       "globalRank" -> globalRank.toString,
                       //                       "localRank" -> localRank.toString,
                       "cudaVisibleDevices" -> nextGpuIndex.toString
                     ).asJava)

       if (nodeToProcessors.contains(node)) {
         nodeToProcessors(node) :+= requiredProcessor
       } else {
         nodeToProcessors(node) = Seq(requiredProcessor)
       }
     }
     var result = nodeToProcessors.flatMap { case (node, processors) =>
       processors.map(p => (p, node))
     }(collection.breakOut)

     resourceApplication.resourceDispatch.appendAll(result)
    resourceApplication
   }

   def  dispatchDeepSpeedInner(worldSize:Int,serverNodes:Array[ErServerNode]):  Array[(ErProcessor, ErServerNode)]  ={
    var nodeResourceTupes = serverNodes.map(n=>(n,n.resources.filter(_.resourceType==ResourceTypes.VGPU_CORE).map(_.getUnAllocatedResource).apply(0)))
      .sortWith(_._2>_._2).toBuffer
    //    // FIXME: evenly distribute processors to nodes for now
    val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
    //
    for (index <- 0 until worldSize) {

      var  nodeTupe = nodeResourceTupes.head
      var  node =  nodeTupe._1
      nodeResourceTupes = (nodeResourceTupes.tail+=nodeTupe.copy(_2=nodeTupe._2-1)).sortWith(_._2>_._2)
      val host = node.endpoint.host
      val globalRank = index
      val localRank = nodeToProcessors.getOrElse(node, Seq()).size
      val processor = ErProcessor(
        serverNodeId = node.id,
        processorType = JobProcessorTypes.DeepSpeed.toString,
        commandEndpoint = ErEndpoint(host, 0),
        status = ProcessorStatus.NEW,
        options = Map(
          "globalRank" -> globalRank.toString,
          "localRank" -> localRank.toString
        ).asJava,
        resources=Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 1,status= ResourceStatus.PRE_ALLOCATED))
      )

      if (nodeToProcessors.contains(node)) {
        nodeToProcessors(node) :+= processor
      } else {
        nodeToProcessors(node) = Seq(processor)
      }
    }
    nodeToProcessors.flatMap { case (node, processors) =>
      processors.map(p => (p, node))
    }(collection.breakOut)
  }

   def dispatchDeepSpeed(worldSize:Int): Array[(ErProcessor, ErServerNode)] =synchronized {
    // cluster nodes
    val serverNodes = serverNodeCrudOperator.getServerNodesWithResource(
      ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
    )
     dispatchDeepSpeedInner(worldSize, serverNodes)
  }


     case class ResourceApplication(
                                    sessionId : String,
                                    sessionName: String =StringConstants.EMPTY,
                                    processors : Array[ErProcessor]=Array[ErProcessor](),
                                   sortByResourceType: String =ResourceTypes.VCPU_CORE,
                                   needDispatch: Boolean= true,
                                   dispatchStrategy:String = DispatchStrategy.SINGLE_NODE_FIRST,
                                   resourceExhaustedStrategy:String =  ResourceExhaustedStrategy.WAITING,
                                   allowExhausted:Boolean = false,
                                   resourceDispatch:ArrayBuffer[(ErProcessor, ErServerNode)]=ArrayBuffer(),
                                   resourceLatch: CountDownLatch = new  CountDownLatch(1),
                                   timeout: Int = 0,
                                    submitTimeStamp:Long = 0,
                                    waitingCount :AtomicInteger = new AtomicInteger(1),
                                    status:AtomicInteger =new AtomicInteger(0),
                                    processorTypes: Array[String]=Array[String](),

                                    options:mutable.Map[String,String]  = mutable.Map[String,String]()
                                  ){
      def  getResult(): Array[(ErProcessor, ErServerNode)] ={
        try{
          if(timeout>0){
            var alreadyGet =  resourceLatch.await(timeout, TimeUnit.MILLISECONDS)
            if(!alreadyGet){
              throw new ErSessionException("dispatch resource timeout")
            }
          }
          else
            resourceLatch.await()
          resourceDispatch.toArray
        }finally {
          waitingCount.decrementAndGet()
        }
      }
      def  countDown():Unit={
        resourceLatch.countDown()
      }
    }

    def  submitResourceRequest(resourceRequest: ResourceApplication):Unit={


      applicationQueue.broker.put(resourceRequest)
    }

    def  checkResourceEnough(erServerNodes:Array[ErServerNode],resourceApplication: ResourceApplication): Boolean={
       var  result = true
      var globalRemainResourceMap: mutable.Map[String,Long] = mutable.Map[String,Long]()
      var nodeRemainResourceMap: mutable.Map[Long,mutable.Map[String,Long]] = mutable.Map[Long,mutable.Map[String,Long]]()

      erServerNodes.foreach(n=>{
        var  nodeMap = nodeRemainResourceMap.getOrElse(n.id, mutable.Map[String,Long]())
        n.resources.foreach(r=>{
          var  remain :Long = nodeMap.getOrElse(r.resourceType,0)
          var  unAllocated:Long = r.getUnAllocatedResource()
          nodeMap(r.resourceType)=remain+ unAllocated
        })
        nodeRemainResourceMap(n.id)=nodeMap

      })


      nodeRemainResourceMap.foreach(e=>{
        e._2.foreach(r=>{
          var count :Long =  globalRemainResourceMap.getOrElse(r._1,0)
          globalRemainResourceMap(r._1)= count+r._2
        })
      })

      if(!resourceApplication.allowExhausted) {
        require(erServerNodes.length>0)
        resourceApplication.dispatchStrategy match {
          case DispatchStrategy.FIX => {
            val eggsPerNode = resourceApplication.options.getOrElse(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, "1")).toInt
            var resourceType= resourceApplication.options.getOrElse("resourceType",ResourceTypes.VCPU_CORE)
            var types = resourceApplication.processorTypes.length
            result=nodeRemainResourceMap.forall(n=>{
              var exist:Long =  n._2.getOrElse(resourceType,0)
              exist>=eggsPerNode*types
            })
          }
          case _ => {
            var processors = resourceApplication.processors
            var erServerNode = erServerNodes.reduce((x, y) => {
              var newResource = (x.resources.toBuffer ++ y.resources).toArray
              x.copy(resources = newResource)
            })
            var requestResourceMap = processors.reduce((x, y) => {
              var newResource = (x.resources.toBuffer ++ y.resources).toArray
              x.copy(resources = newResource)
            }).resources.groupBy(_.resourceType).mapValues(_.reduce((x, y) => {
              x.copy(allocated = x.allocated + y.allocated)
            }).allocated)

            breakable {
              requestResourceMap.foreach((r) => {
                var globalResourceRemain: Long = globalRemainResourceMap.getOrElse(r._1, -1)
                if (globalResourceRemain.intValue() > -1) {
                  logInfo(s"check resource ${r._1}  request ${r._2} remain ${globalResourceRemain}")
                  if ( r._2 > globalResourceRemain) {
                    result = false
                    break()
                  }
                } else {
                  result = false
                  break()
                }
              })
            }
          }
        }
      }
      result
    }

    // download is not use resource dispatch temporarily
    def submitJodDownload(resourceApplication :ResourceApplication  ): ErSessionMeta ={
      try {
        lockSession(resourceApplication.sessionId)
        var  exist = smDao.existSession(resourceApplication.sessionId)
        if(!exist){
          smDao.register(ErSessionMeta(
            id = resourceApplication.sessionId,
            name = resourceApplication.sessionName,
            processors = resourceApplication.processors,
            totalProcCount = resourceApplication.processors.length,
            status = SessionStatus.NEW)
          )
          logInfo("register session over")
        }else{
          throw  new  ErSessionException("session is already created")
        }
      var erSessionInDb =  smDao.getSession(resourceApplication.sessionId)

//        if(erSessionInDb.status!= SessionStatus.NEW) {
//          logError("")
//            return  erSessionInDb
//        }


        resourceApplication.processors.par.foreach(processor => {
          try {
            // TODO:1: add new params?

            val newSessionMeta = erSessionInDb.copy(
              options = erSessionInDb.options ++ Map(ResourceManagerConfKeys.SERVER_NODE_ID -> processor.serverNodeId.toString))
            val nodeManagerClient = new NodeManagerClient(
              ErEndpoint(host = processor.options.get("ip"),
                port = Integer.parseInt(processor.options.get("port"))))
            nodeManagerClient.startContainers(newSessionMeta)
          }catch {
            case e: Exception =>
              logError(" start container error for ")
          }
        })
        val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
        var isStarted = false
        breakable {
          while (System.currentTimeMillis() <= startTimeout) {
            val cur = smDao.getSession(resourceApplication.sessionId)
            if (cur.activeProcCount < resourceApplication.processors.length) {
              Thread.sleep(100)
            } else {
              isStarted = true
              break
            }
          }
        }
        if(!isStarted){
          SessionManagerService.killSession(erSessionInDb,SessionStatus.ERROR)
          val builder = new mutable.StringBuilder()
          val exception = new ErSessionException("create download session failed")
          throw exception
        }
        smDao.updateSessionMain(erSessionInDb.copy(
          status = SessionStatus.ACTIVE, activeProcCount = resourceApplication.processors.length),afterCall = (connection,sessionMeta)=>{
          if (SessionStatus.KILLED.equals(sessionMeta.status)) {
            smDao.batchUpdateSessionProcessor(sessionMeta)
          }
        })
        smDao.getSession(sessionId = resourceApplication.sessionId)
      }finally {
        unlockSession(resourceApplication.sessionId)
      }
    }





    def  main(args: Array[String]) :Unit = {
//        var   temp =  Array(ErServerNode(id = 1,resources = Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 5))),
//          ErServerNode(id = 3,resources = Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 9)))
//        ,
//          ErServerNode(id = 2,resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1),ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 3),
//            ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 3)))
//        )
//        System.err.println(dispatchDeepSpeedInner(7,temp).mkString)

     var processors :Array[ErProcessor]=        Array(ErProcessor(resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,allocated = 1),ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 3))),
       ErProcessor(resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,allocated = 1),ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 2))))

     var resourceApplication: ResourceApplication =  ResourceApplication(sessionId="test",processors=processors,dispatchStrategy=DispatchStrategy.FIX
       ,options = mutable.Map(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE->"1","resourceType"->ResourceTypes.VCPU_CORE))

     println( checkResourceEnough(Array(ErServerNode(id=1,resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total=2,allocated = 0,preAllocated = 1),
       ErResource(resourceType = ResourceTypes.VGPU_CORE,total=2,allocated = 0)))
      ,ErServerNode(id=2,resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total=2,allocated = 0),
         ErResource(resourceType = ResourceTypes.VGPU_CORE,total=2,allocated = 0)))

     ) ,resourceApplication
       ))

    }






}
