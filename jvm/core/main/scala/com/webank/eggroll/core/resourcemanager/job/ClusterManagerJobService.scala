package com.webank.eggroll.core.resourcemanager.job

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.{ClusterResourceManager, SessionMetaDao}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

class ClusterManagerJobService extends Logging {
  private val smDao = new SessionMetaDao

  private def dispatchDeepSpeed(worldSize: Int): Array[(ErProcessor, ErServerNode)] = {
    // cluster nodes
    val serverNodes = new ServerNodeCrudOperator().getServerNodesWithResource(
      ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
    )



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
        var dispatchedProcessors = ClusterResourceManager.dispatchDeepSpeed(worldSize)

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
          val processors = nodeAndProcessors.map(_._1.copy(sessionId = submitJobMeta.id))
          val nodeManagerClient = new NodeManagerClient(node.endpoint)
          ClusterResourceManager.preAllocateResource(processors)
          nodeManagerClient.startJobContainers(submitJobMeta.copy(processors = processors))
        }

        val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
        var isStarted = false
        breakable {
          while (System.currentTimeMillis() <= startTimeout) {
            val cur =     smDao.getSessionMain(submitJobMeta.id)
            if (cur.activeProcCount < submitJobMeta.worldSize) {
              Thread.sleep(100)
            } else {
              isStarted = true
              break
            }
          }
        }

        if (!isStarted) {
          val curDetails = smDao.getSession(submitJobMeta.id)
          // last chance to check
          if (curDetails.activeProcCount < submitJobMeta.worldSize) {
            dispatchedProcessors.groupBy(_._2).par.foreach { case (node, nodeAndProcessors) =>
              val processors = nodeAndProcessors.map(_._1.copy(sessionId = submitJobMeta.id))
              val nodeManagerClient = new NodeManagerClient(node.endpoint)
              ClusterResourceManager.preAllocateResource(processors)
              nodeManagerClient.killJobContainers(submitJobMeta.copy(processors = processors))
            }

            val builder = new mutable.StringBuilder()
            builder.append(s"unable to start all processors for session id: '${submitJobMeta.id}'. ")
              .append(s"Please check corresponding bootstrap logs at '${CoreConfKeys.EGGROLL_LOGS_DIR.get()}/${submitJobMeta.id}' to check the reasons. Details:\n")
              .append("=================\n")
              .append(s"total processors: ${curDetails.totalProcCount}, \n")
              .append(s"started count: ${curDetails.activeProcCount}, \n")
              .append(s"not started count: ${curDetails.totalProcCount - curDetails.activeProcCount}, \n")
//              .append(s"current active processors per node: ${activesPerNode}, \n")
//              .append(s"not started processors and their nodes: ${inactivesToNode}")
            val exception = new ErSessionException(builder.toString())
            throw exception
          }
        }



        // FIXME: update?
        smDao.updateSessionMain(ErSessionMeta(
          status = SessionStatus.ACTIVE, activeProcCount = worldSize))
        submitJobMeta.copy(status = SessionStatus.ACTIVE)
      case _ =>
        throw new IllegalArgumentException(s"unsupported job type: ${submitJobMeta.jobType}")
    }
  }
}
