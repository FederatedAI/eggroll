package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.{DispatchStrategy, NodeManagerConfKeys, ProcessorStatus, ResourceOperationStauts, ResourceOperationType, ResourceStatus, ResourceTypes, ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.datastructure.FifoBroker
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErResourceAllocation, ErServerNode}
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.{ResourceApplication, dispatchDeepSpeedInner, serverNodeCrudOperator}
import com.webank.eggroll.core.resourcemanager.job.JobProcessorTypes
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.collection.{breakOut, mutable}
import scala.math.Numeric.LongIsIntegral
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object ClusterResourceManager extends Logging{

    val   applicationQueue =  new FifoBroker[ResourceApplication]

    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()

    var  dispatchThread = new  Thread(()=>{
      println("dispatch thread start")
      while(true){
        try{
         var  resourceApplication = applicationQueue.next()
          handleResourceApplication(resourceApplication)
        }catch {
          case  e:Exception =>  e.printStackTrace()
        }
      }
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
//       nodeResourceTupes = (nodeResourceTupes.tail += nodeTupe.copy(_2 = nodeTupe._2 - 1)).sortWith(_._2 > _._2)
     //  checkResource()

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

   private def  handleResourceApplication(resourceApplication: ResourceApplication): ResourceApplication = {
     var  resultResourceApplication :ResourceApplication = resourceApplication
     if(resourceApplication.needDispatch){
       var serverNodes = getServerNodeWithResource();
       checkResource(serverNodes,resourceApplication.processors,resourceApplication.allowExhausted);
       resultResourceApplication = resourceApplication.dispatchStrategy match {
         case  DispatchStrategy.REMAIN_MOST_FIRST => remainMostFirstDispatch(serverNodes,resourceApplication);
         case  DispatchStrategy.RANDOM =>  randomDispatch(serverNodes,resultResourceApplication);
       }
     }
     preAllocateResource(resultResourceApplication.processors)
     resultResourceApplication.countDownLatch.countDown()
     resultResourceApplication
    }



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

  def preAllocateResource(processors: Array[ErProcessor]): Unit = synchronized {

    ServerNodeCrudOperator.dbc.withTransaction(conn => {
      serverNodeCrudOperator.insertProcessorResource(conn, processors);
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


    case class ResourceApplication(processors : Array[ErProcessor],
                                  sortByResourceType: String =ResourceTypes.VCPU_CORE,
                                  needDispatch: Boolean= false,
                                  dispatchStrategy:String = DispatchStrategy.REMAIN_MOST_FIRST,
                                  allowExhausted:Boolean = true,
                                  resourceDispatch:ArrayBuffer[(ErProcessor, ErServerNode)]=ArrayBuffer(),
                                  countDownLatch: CountDownLatch){
      def  getResult(): Array[(ErProcessor, ErServerNode)] ={
        countDownLatch.await()
        resourceDispatch.toArray
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
