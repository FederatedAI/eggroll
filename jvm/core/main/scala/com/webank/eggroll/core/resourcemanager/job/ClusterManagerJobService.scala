package com.webank.eggroll.core.resourcemanager.job

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.SessionMetaDao
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable
import scala.util.Random

class ClusterManagerJobService extends Logging {
  private val smDao = new SessionMetaDao

  private def dispatchDeepSpeed(worldSize: Int): Array[(ErProcessor, ErServerNode)] = {
    // cluster nodes
    val serverNodes = new ServerNodeCrudOperator().getServerNodes(
      ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
    ).serverNodes

    val shuffledNodes = Random.shuffle(serverNodes.toSeq)
    // dispatch processors
    // FIXME: evenly distribute processors to nodes for now
    val nodeToProcessors = mutable.Map[ErServerNode, Seq[ErProcessor]]()

    for (index <- 0 until worldSize) {
      val node = shuffledNodes(index % shuffledNodes.size)
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
        ).asJava
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

  def submitJob(submitJobMeta: ErJobMeta): ErJobMeta = {
    JobProcessorTypes.fromString(submitJobMeta.jobType) match {
      case Some(JobProcessorTypes.DeepSpeed) =>
        val worldSize = submitJobMeta.worldSize
        var dispatchedProcessors = dispatchDeepSpeed(worldSize)

        // FIXME: just retrieve updated processors' id
        smDao.register(ErSessionMeta(
          id = submitJobMeta.id,
          processors = dispatchedProcessors.map(_._1),
          totalProcCount = worldSize,
          status = SessionStatus.NEW)
        )
        val registeredSessionMeta = smDao.getSession(submitJobMeta.id)
        dispatchedProcessors = dispatchedProcessors.zip(registeredSessionMeta.processors).map {
          case ((processor, node), registeredProcessor) =>
            (processor.copy(id = registeredProcessor.id), node)
        }

        // start containers
        dispatchedProcessors.groupBy(_._2).par.foreach { case (node, nodeAndProcessors) =>
          val processors = nodeAndProcessors.map(_._1)
          val nodeManagerClient = new NodeManagerClient(node.endpoint)
          nodeManagerClient.startJobContainers(submitJobMeta.copy(processors = processors))
        }

        // FIXME: update?
        smDao.updateSessionMain(ErSessionMeta(
          status = SessionStatus.ACTIVE, activeProcCount = worldSize))
        submitJobMeta.copy(status = SessionStatus.ACTIVE)
      case None =>
        throw new IllegalArgumentException(s"unsupported job type: ${submitJobMeta.jobType}")
    }
  }
}
