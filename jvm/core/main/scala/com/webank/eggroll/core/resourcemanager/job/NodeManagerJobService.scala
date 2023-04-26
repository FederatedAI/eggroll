package com.webank.eggroll.core.resourcemanager.job

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{ProcessorStatus, ProcessorTypes, ResourceManagerConfKeys}
import com.webank.eggroll.core.meta.{ErJobMeta, ErProcessor}
import com.webank.eggroll.core.resourcemanager.NodeManagerMeta
import com.webank.eggroll.core.resourcemanager.job.container.{ContainersManager, DeepSpeedContainer}
import com.webank.eggroll.core.session.RuntimeErConf

import scala.concurrent.ExecutionContext


class NodeManagerJobService(implicit ec: ExecutionContext) {

  var  client = new  ClusterManagerClient()
  private val containersManager = ContainersManager.builder()
    // TODO: status callbacks here
    .withStartedCallback((container) => {
//      object ProcessorStatus {
//        val NEW = "NEW"
//        val RUNNING = "RUNNING"
//        val STOPPED = "STOPPED"
//        val KILLED = "KILLED"
//        val ERROR = "ERROR"
//      }


      println(s"container started: ${container}")

      client.heartbeat(ErProcessor(id=container.getProcessorId(),serverNodeId = NodeManagerMeta.serverNodeId,status =ProcessorStatus.RUNNING ));

    })
    .withSuccessCallback((container) => {
      println(s"container success: ${container}")
      client.heartbeat(ErProcessor(id=container.getProcessorId(),serverNodeId = NodeManagerMeta.serverNodeId,status =ProcessorStatus.STOPPED ));

    })
    .withFailedCallback((container) => {
      println(s"container failed: ${container}")
      client.heartbeat(ErProcessor(id=container.getProcessorId(),serverNodeId = NodeManagerMeta.serverNodeId,status =ProcessorStatus.ERROR ));
    })
    .withExceptionCallback((container, e) => {
      println(s"container exception: ${container}, ${e}")
      client.heartbeat(ErProcessor(id=container.getProcessorId(),serverNodeId = NodeManagerMeta.serverNodeId,status =ProcessorStatus.KILLED ));
    })
    .build

  def startJobContainers(submitJobMeta: ErJobMeta): ErJobMeta = {
    println("startJobContainers")
    operateContainers(submitJobMeta, "start")
  }

  def stopJobContainers(sessionMeta: ErJobMeta): ErJobMeta = {
    operateContainers(sessionMeta, "stop")
  }

  def killJobContainers(sessionMeta: ErJobMeta): ErJobMeta = {
    operateContainers(sessionMeta, "kill")
  }

  private def operateContainers(submitJobMeta: ErJobMeta, opType: String): ErJobMeta = {
    JobProcessorTypes.fromString(submitJobMeta.jobType) match {
      case Some(jobType) =>
        val runtimeConf = new RuntimeErConf(submitJobMeta)
        submitJobMeta.processors.par.foreach { p =>
          val containerId = (p.processorType, p.id, p.serverNodeId).hashCode()
          opType match {
            case "start" => {
              val container = jobType match {
                case JobProcessorTypes.DeepSpeed =>
                  val localRank = p.options.getOrDefault("localRank", "-1").toInt
                  val globalRank = p.options.getOrDefault("globalRank", "-1").toInt
                  if (localRank == -1 || globalRank == -1) {
                    throw new IllegalArgumentException(s"localRank or globalRank not set: ${p.options}")
                  }
                  new DeepSpeedContainer(
                    processorId = p.id,
                    conf = runtimeConf,
                    localRank = localRank,
                    globalRank = globalRank,
                    commandArguments = submitJobMeta.commandArguments,
                    environmentVariables = submitJobMeta.environmentVariables,
                    files = submitJobMeta.files,
                    zippedFiles = submitJobMeta.zippedFiles,
                    containerId = containerId.toString
                  )
              }
              containersManager.addContainer(containerId, container)
              containersManager.startContainer(containerId)
            }
            case "stop" => containersManager.stopContainer(containerId)
            case "kill" => containersManager.killContainer(containerId)
            case _ => throw new IllegalArgumentException(s"op not supported: '${opType}'")
          }
        }
        submitJobMeta
      case None =>
        throw new IllegalArgumentException(s"jobType not supported: '${submitJobMeta.jobType}'")
    }
  }
}
