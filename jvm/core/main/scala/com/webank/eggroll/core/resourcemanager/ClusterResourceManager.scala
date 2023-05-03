package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.{NodeManagerConfKeys, ProcessorStatus, ResourceOperationStauts, ResourceOperationType, ResourceStatus, ResourceTypes, ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErResourceAllocation, ErServerNode}
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.serverNodeCrudOperator
import com.webank.eggroll.core.resourcemanager.job.JobProcessorTypes
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging

import java.sql.Connection
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable
import scala.math.Numeric.LongIsIntegral
import scala.util.Random

object ClusterResourceManager extends Logging{

    lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()

    def  clusterAllocateResource(sessionId :String,processors :Array[ErProcessor] ) : Unit={
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

  def  allocateResource(processors: Array[ErProcessor] ) : Unit=synchronized{
    ServerNodeCrudOperator.dbc.withTransaction(conn=> {
      var allocateResourceProcessor = processors.map(p => {
        logInfo(s"=================== ${p}")
        p.copy(resources = serverNodeCrudOperator.queryProcessorResource(conn,p,ResourceStatus.PRE_ALLOCATED).map(_.copy(status=ResourceStatus.ALLOCATED)))
      })
      var flatedResource = flatResources(allocateResourceProcessor)
      logInfo(s"flated resource ${flatedResource}")
      allocateResourceProcessor.foreach(p=> {
        serverNodeCrudOperator.updateProcessorResource(conn, p);
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








  def  returnResource(processors: Array[ErProcessor],f:(Connection)=>Unit =null ):  Unit=synchronized{
    logInfo("===================================returnResource {}")
    ServerNodeCrudOperator.dbc.withTransaction(conn=> {
      try {
        if (f != null) {
          f(conn)
        }

      processors.foreach(p => {
        var resourceInDb = serverNodeCrudOperator.queryProcessorResource(conn, p, ResourceStatus.ALLOCATED)
        resourceInDb.foreach(r => {
          logInfo(s"processor ${p.id} return resource ${r}")
        })

        if (resourceInDb.length > 0) {
          serverNodeCrudOperator.updateProcessorResource(conn, p.copy(resources = resourceInDb.map(_.copy(status = ResourceStatus.RETURN))))
          serverNodeCrudOperator.returnNodeResource(conn, p.serverNodeId, resourceInDb)
        }
      })
    }catch{
      case e :Exception =>{
        e.printStackTrace()
      }


    }
    })

  }

    private def  flatResources(processors: Array[ErProcessor]): Map[Long, Array[ErResource]] ={
      processors.groupBy(_.serverNodeId).mapValues(
        _.flatMap(_.resources).groupBy(_.resourceType).mapValues(_.reduce((x,y)=>{
          x.copy(allocated=x.allocated+y.allocated)
        })).values.toArray
      )
    }


   def dispatchDeepSpeed(worldSize:Int): Array[(ErProcessor, ErServerNode)] =synchronized {
    // cluster nodes
    val serverNodes = serverNodeCrudOperator.getServerNodesWithResource(
      ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
    )
    logInfo(s"lllllllllllllllllllll ${serverNodes}");

   //  var nodeResourceTupes = serverNodes.map(n=>(n,n.resources.filter(r=>r.resourceType==ResourceTypes.VGPU_CORE).map(r=>{r.total-r.allocated}).apply(0))).sortWith(_._2>_._2).toBuffer
     var nodeResourceTupes = serverNodes.map(n=>(n,n.resources.filter(r=>r.resourceType==ResourceTypes.VGPU_CORE).map(_.getUnAllocatedResource).apply(0))).sortWith(_._2>_._2).toBuffer
    logInfo(s"kkkkkkkkkkkkkkkkkkkkk ${nodeResourceTupes}")
     //    // FIXME: evenly distribute processors to nodes for now
    val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()
//
    for (index <- 0 until worldSize) {
    //  val node = shuffledNodes(index % shuffledNodes.size)
      var  nodeTupe = nodeResourceTupes.remove(0)
      var  node =  nodeTupe._1
      nodeResourceTupes+= nodeTupe.copy(_2=nodeTupe._2-1)
      nodeResourceTupes =nodeResourceTupes.sortWith(_._2>_._2)

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
        resources=Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 1))
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

    def  checkResource(processors :Array[ErProcessor]): Boolean={
       var   resourceMap = flatResources(processors)
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


//    def  reduceResource(nodeId:Long,resources:Array[ ErResource]): Unit ={
//      serverNodeCrudOperator.allocateNodeResource(nodeId,resources)
//    }


//    def  main(args: Array[String]) :Unit = {
//        var   temp =  Array(ErServerNode(id = 1,resources = Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 5)))
//        ,
//          ErServerNode(id = 2,resources = Array(ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 1),ErResource(resourceType = ResourceTypes.VCPU_CORE,total = 3),ErResource(resourceType = ResourceTypes.VGPU_CORE,total = 3)))
//        )
//        dispatchDeepSpeed(temp)
//
//    }

}
