package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.containers.JobProcessorTypes
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.{DispatchStrategy, NodeManagerConfKeys, ProcessorStatus, ResourceEventType, ResourceExhaustedStrategy, ResourceOperationStauts, ResourceOperationType, ResourceStatus, ResourceTypes, ServerNodeStatus, ServerNodeTypes, SessionStatus}
import com.webank.eggroll.core.datastructure.FifoBroker
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErResourceAllocation, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.{ResourceApplication, dispatchDeepSpeedInner, serverNodeCrudOperator}

import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErServerNode}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{breakOut, mutable}
import scala.math.Numeric.LongIsIntegral
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object ClusterResourceManager extends Logging{

    val   applicationQueue   =   new FifoBroker[ResourceApplication]
    var   resourceEventQueue = new FifoBroker[ResourceEvent]
    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()
    private val smDao = new SessionMetaDao
    var  dispatchThread = new  Thread(()=>{
      while(true){
        var resourceApplication:ResourceApplication =null
        if(applicationQueue.broker.size()>0)
            resourceApplication = applicationQueue.broker.peek()
        else {
         // println(s"dispatch thread peek====${resourceApplication}")
          Thread.sleep(500)
        }
        try{
          breakable {
            if(resourceApplication!=null) {
              var now = System.currentTimeMillis()
              if (resourceApplication.needDispatch) {
                if (resourceApplication.waitingCount.get() == 0
                ) {
                  //过期资源申请
                  logError("expired resource request  !!!!!!!!!!!!!")
                  applicationQueue.broker.poll()
                  break()
                }
                var serverNodes = getServerNodeWithResource();
                var enough = checkResource(serverNodes, resourceApplication.processors, resourceApplication.allowExhausted)
                logInfo(s"resource is enough ? ${enough}")
                if (!enough) {
                  resourceApplication.resourceExhaustedStrategy match {
                    case ResourceExhaustedStrategy.IGNORE => ;

                    case ResourceExhaustedStrategy.WAITING =>
                      Thread.sleep(1000)
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
                }
              }
              logInfo(s"========================${resourceApplication.processors.mkString}")

              var dispatchedProcessors = resourceApplication.resourceDispatch
                //.toArray.map(_._1)

              smDao.register(ErSessionMeta(
                id = resourceApplication.sessionId,
                processors = dispatchedProcessors.toArray.map(_._1),
                totalProcCount = dispatchedProcessors.length,
                status = SessionStatus.NEW)
              )
              logInfo("register session  over")

              val registeredSessionMeta = smDao.getSession(resourceApplication.sessionId)
              dispatchedProcessors = dispatchedProcessors.zip(registeredSessionMeta.processors).map {
                case ((processor, node), registeredProcessor) =>
                  (processor.copy(id = registeredProcessor.id), node)
              }
              resourceApplication.resourceDispatch.clear()
              resourceApplication.resourceDispatch.appendAll(dispatchedProcessors)
              //ProcessorStateMachine.
              preAllocateResource(dispatchedProcessors.toArray.map(_._1))
              resourceApplication.countDown()
              applicationQueue.broker.remove()

            }

          }
        }catch {
          case  e:Exception => {
            e.printStackTrace()

          }

        }

      }
      logInfo("!!!!!!!!!!!!!!!!!!!dispatch thread quit!!!!!!!!!!!!!!!!")

    })
    dispatchThread.start()
   private def  randomDispatch(serverNodes:Array[ErServerNode] ,resourceApplication: ResourceApplication): ResourceApplication ={

     var requiredProcessors = resourceApplication.processors;

//     var nodeResourceTupes = serverNodes.map(n => (n, n.resources.filter(_.resourceType == resourceApplication.sortByResourceType).map(_.getUnAllocatedResource).apply(0)))
//       .sortWith(_._2 > _._2).toBuffer

     val shuffledNodes = Random.shuffle(serverNodes.toSeq)

     val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
     //
     for (index <- 0 until requiredProcessors.length) {
       //System.err.println(nodeResourceTupes.map(_._2))
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
     //resourceApplication.resourceDispatch+=result
     //resourceApplication.copy(resourceDispatch = result.toArray)
     resourceApplication.resourceDispatch.appendAll(result)
     resourceApplication
   }

  private def getServerNodeWithResource():Array[ErServerNode]={
    serverNodeCrudOperator.getServerNodesWithResource(
      ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER))


  }

  private def  remainMostFirstDispatch(serverNodes:Array[ErServerNode],resourceApplication: ResourceApplication): ResourceApplication = {
     logInfo("=============remainMostFirstDispatch")
     var requiredProcessors = resourceApplication.processors;
     var nodeResourceTupes = serverNodes.map(n => (n, n.resources.filter(_.resourceType == resourceApplication.sortByResourceType).map(_.getUnAllocatedResource).apply(0)))
       .sortWith(_._2 > _._2).toBuffer

     //    // FIXME: evenly distribute processors to nodes for now
     val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
     //
     for (index <- 0 until requiredProcessors.length) {

       var requiredProcessor = requiredProcessors(index)

       var nodeTupe = nodeResourceTupes.head
       var node = nodeTupe._1

       nodeResourceTupes = (nodeResourceTupes.tail += nodeTupe.copy(_2 = nodeTupe._2 - 1)).sortWith(_._2 > _._2)
       val host = node.endpoint.host
       val globalRank = index
       val localRank = nodeToProcessors.getOrElse(node, Seq()).size
       requiredProcessor = requiredProcessor.copy(serverNodeId = node.id,commandEndpoint = ErEndpoint(host, 0),
                     options = Map(
                       "globalRank" -> globalRank.toString,
                       "localRank" -> localRank.toString
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

    logInfo(s"========dispatch result ${result}")
     resourceApplication.resourceDispatch.appendAll(result)
    resourceApplication
   }

//   private def  handleResourceApplication(resourceApplication: ResourceApplication): ResourceApplication = synchronized{
//     var  resultResourceApplication :ResourceApplication = resourceApplication
//     var now  = System.currentTimeMillis()
//     if(resourceApplication.needDispatch){
//       if(resourceApplication.timeout>0&&
//         resourceApplication.submitTimeStamp+resourceApplication.timeout>now&&
//         resourceApplication.waitingCount.get()==0
//       ){
//         //过期资源申请
//       }
//
//       var serverNodes = getServerNodeWithResource();
//
//       var enough = checkResource(serverNodes,resourceApplication.processors,resourceApplication.allowExhausted)
//       if(!enough) {
//         resourceApplication.resourceExhaustedStrategy match {
//           case ResourceExhaustedStrategy.IGNORE => ;
//           case ResourceExhaustedStrategy.WAITING =>
//           case ResourceExhaustedStrategy.THROW_ERROR => throw Exception
//         }
//       }
//
//       resultResourceApplication = resourceApplication.dispatchStrategy match {
//         case  DispatchStrategy.REMAIN_MOST_FIRST => remainMostFirstDispatch(serverNodes,resourceApplication);
//         case  DispatchStrategy.RANDOM =>  randomDispatch(serverNodes,resultResourceApplication);
//       }
//     }
//    // preAllocateResource(resultResourceApplication.processors)
//     resultResourceApplication.resourceLatch.countDown()
//     resultResourceApplication
//    }


  //废弃
  def  allocateResource(processors: Array[ErProcessor] ,beforeCall:(Connection,ErProcessor)=>Unit =null,afterCall:(Connection,ErProcessor)=>Unit =null) : Unit=synchronized{
    ServerNodeCrudOperator.dbc.withTransaction(conn=> {



      var allocateResourceProcessor = processors.map(p => {
        p.copy(resources = serverNodeCrudOperator.queryProcessorResource(conn,p,ResourceStatus.PRE_ALLOCATED).map(_.copy(status=ResourceStatus.ALLOCATED)))
      })
      var flatedResource = flatResources(allocateResourceProcessor)
      logInfo(s"flated resource ${flatedResource}")
      allocateResourceProcessor.foreach(p=> {
        if(beforeCall!=null)
            beforeCall(conn,p)
        serverNodeCrudOperator.updateProcessorResource(conn, p);
        if(afterCall!=null)
            afterCall(conn,p)
      })

      flatedResource.foreach(e => {
        e._2.foreach(resource=>{
          logInfo(s"allocate resource to node  ${e._1} ${resource}")
        })

       // serverNodeCrudOperator.allocateNodeResource(conn,e._1, e._2)
       var  erResources =  serverNodeCrudOperator.countNodeResource(conn,e._1)
        serverNodeCrudOperator.updateNodeResource(conn,e._1,erResources)
      })

    })
  }
  //废弃
  def preAllocateResource(processors: Array[ErProcessor]): Unit = synchronized {
    logInfo(s"============== preAllocateResource ============${processors.mkString}")
    ServerNodeCrudOperator.dbc.withTransaction(conn => {
      serverNodeCrudOperator.insertProcessorResource(conn, processors);
      processors.foreach(p=>{
        var  nodeResources =  serverNodeCrudOperator.countNodeResource(conn,p.serverNodeId)
        var  needUpdateResources = nodeResources.map(r=>r.copy(preAllocated = r.allocated,allocated = -1))
        serverNodeCrudOperator.updateNodeResource(conn,p.serverNodeId,needUpdateResources)
      })

    })
  }

  def  returnResource(processors: Array[ErProcessor],beforeCall:(Connection,ErProcessor)=>Unit =null,afterCall:(Connection,ErProcessor)=>Unit =null):  Unit=synchronized{
    logInfo(s"return resource ${processors.mkString("<",",",">")}")
    ServerNodeCrudOperator.dbc.withTransaction(conn=> {
      try {

      processors.foreach(p => {

        if (beforeCall != null) {
          beforeCall(conn,p)
        }
        var resourceInDb = serverNodeCrudOperator.queryProcessorResource(conn, p, ResourceStatus.ALLOCATED)
        resourceInDb.foreach(r => {
          logInfo(s"processor ${p.id} return resource ${r}")
        })
        if (resourceInDb.length > 0) {
          serverNodeCrudOperator.updateProcessorResource(conn, p.copy(resources = resourceInDb.map(_.copy(status = ResourceStatus.RETURN))))
          serverNodeCrudOperator.returnNodeResource(conn, p.serverNodeId, resourceInDb)
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
    }
    )

    logInfo("==============over========")

  }

    private def  flatResources(processors: Array[ErProcessor]): Map[Long, Array[ErResource]] ={
      processors.groupBy(_.serverNodeId).mapValues(
        _.flatMap(_.resources).groupBy(_.resourceType).mapValues(_.reduce((x,y)=>{
          x.copy(allocated=x.allocated+y.allocated)
        })).values.toArray
      )
    }

   def  dispatchDeepSpeedInner(worldSize:Int,serverNodes:Array[ErServerNode]):  Array[(ErProcessor, ErServerNode)]  ={
    var nodeResourceTupes = serverNodes.map(n=>(n,n.resources.filter(_.resourceType==ResourceTypes.VGPU_CORE).map(_.getUnAllocatedResource).apply(0)))
      .sortWith(_._2>_._2).toBuffer
    //    // FIXME: evenly distribute processors to nodes for now
    val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
    //
    for (index <- 0 until worldSize) {
      System.err.println(nodeResourceTupes.map(_._2))
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
                                    processors : Array[ErProcessor],
                                  sortByResourceType: String =ResourceTypes.VCPU_CORE,
                                  needDispatch: Boolean= false,
                                  dispatchStrategy:String = DispatchStrategy.REMAIN_MOST_FIRST,
                                   resourceExhaustedStrategy:String =  ResourceExhaustedStrategy.WAITING,
                                  allowExhausted:Boolean = true,
                                  resourceDispatch:ArrayBuffer[(ErProcessor, ErServerNode)]=ArrayBuffer(),
                                   resourceLatch: CountDownLatch,
                                   timeout: Int = 0,
                                    submitTimeStamp:Long = 0,
                                    waitingCount :AtomicInteger = new AtomicInteger(1),
                                    status:AtomicInteger =new AtomicInteger(0)

                                  ){
      def  getResult(): Array[(ErProcessor, ErServerNode)] ={
        try{
          if(timeout>0)
            resourceLatch.await(timeout, TimeUnit.MILLISECONDS)
          else
            resourceLatch.await()
          resourceDispatch.toArray
        }finally {
          waitingCount.decrementAndGet()
        }
      }
      def  countDown():Unit={
        logInfo("=============countDown==============")
        resourceLatch.countDown()
      }


    }

    def  submitResourceRequest(resourceRequest: ResourceApplication):Unit={
      applicationQueue.broker.put(resourceRequest)
    }

    def  checkResource(erServerNodes:Array[ErServerNode],processors :Array[ErProcessor],allowExhauted:Boolean): Boolean={
       var  result = true
      require(erServerNodes.length>0)
     var  erServerNode = erServerNodes.reduce((x,y)=>{
        var  newResource = (x.resources.toBuffer++y.resources).toArray
        x.copy(resources =newResource)
      })
      var   requestResourceMap  = processors.reduce((x,y)=>{
        var  newResource = (x.resources.toBuffer++y.resources).toArray
        x.copy(resources = newResource)
      }).resources.groupBy(_.resourceType).mapValues(_.reduce((x,y)=>{
        x.copy(allocated=x.allocated+y.allocated)
      }).allocated)


//       var   requestResourceMap = unnionProcessor.resources.groupBy(_.resourceType).mapValues(_.reduce((x,y)=>{
//         x.copy(allocated=x.allocated+y.allocated)
//       }).allocated)

      println("=========="+requestResourceMap)

      var nodeResourceMap:Map[String,Long] =  erServerNode.resources.groupBy(_.resourceType).mapValues(_.reduce((x,y)=>{
        x.copy(allocated=x.allocated+y.allocated)
      }).getUnAllocatedResource())
      breakable {
        requestResourceMap.foreach((r) => {
          var nodeResourceRemain:Long = nodeResourceMap.getOrElse(r._1,-1)
          //println(nodeResourceRemain)
          if (nodeResourceRemain.intValue() > -1) {
            println(s"check resource ${r._1}  request ${r._2} remain ${nodeResourceRemain}")
            logInfo(s"check resource ${r._1}  request ${r._2} remain ${nodeResourceRemain}")

            if (!allowExhauted && r._2 > nodeResourceRemain) {
              result= false
              break()
            }
          } else {
              result= false
              break()
          }
        })
      }
      result
    }
    def  checkResouce():Unit={

    }



    def  main(args: Array[String]) :Unit = {
//        var   temp =  Array(ErServerNode(id = 1,resources = Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 5))),
//          ErServerNode(id = 3,resources = Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 9)))
//        ,
//          ErServerNode(id = 2,resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1),ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 3),
//            ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 3)))
//        )
//        System.err.println(dispatchDeepSpeedInner(7,temp).mkString)



     println( checkResource(Array(ErServerNode(resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total=2,allocated = 0),
       ErResource(resourceType = ResourceTypes.VGPU_CORE,total=2,allocated = 0)))) ,
       Array(ErProcessor(resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,allocated = 1),ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 3))),
         ErProcessor(resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,allocated = 1),ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 2)))),
       true))

    }

}
