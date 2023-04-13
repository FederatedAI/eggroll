package com.webank.eggroll.core.resourcemanager.job

import com.webank.eggroll.core.constant.{ProcessorTypes, ResourceManagerConfKeys}
import com.webank.eggroll.core.meta.ErJobMeta
import com.webank.eggroll.core.resourcemanager.job.container.{ContainersManager, DeepSpeedContainer, EggPairContainer}
import com.webank.eggroll.core.session.RuntimeErConf

import scala.concurrent.ExecutionContext


class NodeManagerJobService(implicit ec: ExecutionContext) {
  private val containersManager = ContainersManager.builder()
    // TODO: status callbacks here
    .withStartedCallback((container) => {
      println(s"container started: ${container}")
    })
    .withSuccessCallback((container) => {
      println(s"container success: ${container}")
    })
    .withFailedCallback((container) => {
      println(s"container failed: ${container}")
    })
    .withExceptionCallback((container, e) => {
      println(s"container exception: ${container}, ${e}")
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
                  new DeepSpeedContainer(runtimeConf, localRank, globalRank, p.id)
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
