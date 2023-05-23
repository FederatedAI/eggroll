package com.webank.eggroll.core.deepspeed.job

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.containers.JobProcessorTypes
import com.webank.eggroll.core.containers.meta._
import com.webank.eggroll.core.deepspeed.job.meta._
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.ResourceApplication
import com.webank.eggroll.core.resourcemanager.ProcessorStateMachine.defaultSessionCallback
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.resourcemanager.{ClusterManagerService, ClusterResourceManager, ProcessorEvent, SessionMetaDao}
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import java.util.concurrent.CountDownLatch
import scala.util.control.Breaks.{break, breakable}

object JobServiceHandler extends Logging {
  private val smDao = new SessionMetaDao

//  ClusterManagerService.registerProcessorCallback(JobProcessorTypes.DeepSpeed.toString, (event: ProcessorEvent) => {
//    new Thread(() => {
//      event match {
//        case ProcessorEvent(eventType, processor) if eventType == ProcessorEventType.PROCESSOR_LOSS =>
//          logDebug(s"processor ${processor.id} lost, kill job ${processor.sessionId}")
//          killJob(processor.sessionId, isTimeout = false)
//      }
//    }).start()
//  })



  def handleJobKill(killJobRequest: KillJobRequest): KillJobResponse = {
    val sessionId = killJobRequest.sessionId
    killJob(sessionId, isTimeout = false)
    KillJobResponse(sessionId)
  }

  def handleJobStop(stopJobRequest: StopJobRequest): StopJobResponse = {
    val sessionId = stopJobRequest.sessionId
    killJob(sessionId, isTimeout = false)
    StopJobResponse(sessionId)
  }

  def handleJobQuery(queryJobRequest: QueryJobRequest): QueryJobResponse = {
    val sessionId = queryJobRequest.sessionId
    val status = smDao.getSessionMain(sessionId).status
    val processors = smDao.getSession(sessionId).processors
    QueryJobResponse(sessionId = sessionId, status = status, processors = processors)
  }

  def handleJobStatusQuery(queryJobStatusRequest: QueryJobStatusRequest): QueryJobStatusResponse = {
    val sessionId = queryJobStatusRequest.sessionId
    val status = smDao.getSessionMain(sessionId).status
    QueryJobStatusResponse(sessionId = sessionId, status = status)
  }

  def handleJobDownload(downloadJobRequest: DownloadJobRequest): DownloadJobResponse = {
    val sessionId = downloadJobRequest.sessionId
    val serverNodeCrudOperator = new ServerNodeCrudOperator()
    val containerContents = smDao.getRanks(sessionId).flatMap { case (containerId, nodeId, globalRank, localRank) =>
      val index = if (downloadJobRequest.ranks.isEmpty) globalRank else downloadJobRequest.ranks.indexOf(globalRank)
      if (index >= 0) {
        Some((nodeId, containerId, globalRank, localRank, index))
      } else {
        None
      }
    }.groupBy(_._1).par.flatMap { case (nodeId, ranks) =>
      val node = serverNodeCrudOperator.getServerNode(ErServerNode(id = nodeId))
      val indexes = ranks.map(_._5)
      indexes.zip(
        try {
          new NodeManagerClient(node.endpoint).downloadContainers(
            DownloadContainersRequest(
              sessionId = sessionId,
              containerIds = ranks.map(_._2),
              compressMethod = downloadJobRequest.compressMethod
            )).containerContents
        } catch {
          case e: Exception =>
            e.printStackTrace()
            ranks.map(r => ContainerContent(
              containerId = r._2,
              content = Array.empty,
              compressMethod = downloadJobRequest.compressMethod))
        }
      )
    }.toArray.sortBy(_._1).map(_._2)
    DownloadJobResponse(sessionId = sessionId, containerContents = containerContents)
  }

  private def waitSubmittedContainers(sessionId: String, expectedWorldSize: Int, timeout: Long): Array[ErProcessor] = {
    var isStarted = false
    breakable {
      while (System.currentTimeMillis() <= timeout) {
        val cur = smDao.getSessionMain(sessionId)
        if (cur.activeProcCount < expectedWorldSize) {
          // assert no container error
          smDao.getSession(sessionId).processors.foreach { processor =>
            if (processor.status == ProcessorStatus.ERROR) {
              throw new ErSessionException(s"processor ${processor.id} failed to start")
            }
          }
          Thread.sleep(100)
        } else {
          isStarted = true
          break
        }
      }
    }
    if (!isStarted) {
      val activeCount = smDao.getSessionMain(sessionId).activeProcCount
      if (activeCount < expectedWorldSize) {
        try {
          killJob(sessionId, isTimeout = true)
        } catch {
          case e: Exception =>
            logError(s"failed to kill job $sessionId", e)
        }
        throw new ErSessionException(
          s"unable to start all processors for session '$sessionId', " +
            s"expected world size: $expectedWorldSize, " +
            s"active world size: $activeCount")
      }
    }
    smDao.getSession(sessionId).processors
  }

  def handleSubmit(submitJobMeta: SubmitJobRequest): SubmitJobResponse = {
    JobProcessorTypes.fromString(submitJobMeta.jobType) match {
      case Some(JobProcessorTypes.DeepSpeed) =>
        handleDeepspeedSubmit(submitJobMeta)
      case _ =>
        throw new IllegalArgumentException(s"unsupported job type: ${submitJobMeta.jobType}")
    }
  }

  private def handleDeepspeedSubmit(submitJobRequest: SubmitJobRequest): SubmitJobResponse = {
    val sessionId = submitJobRequest.sessionId

    val worldSize = submitJobRequest.worldSize

    // prepare processors
    val prepareProcessors = Array.fill(worldSize)(ErProcessor(
      processorType = JobProcessorTypes.DeepSpeed.toString,
      status = ProcessorStatus.NEW,
      resources = Array(ErResource(
        resourceType = ResourceTypes.VGPU_CORE,
        allocated = 1,
        status = ResourceStatus.PRE_ALLOCATED))
    ))
    val resourceApplication = ResourceApplication(

      processors = prepareProcessors,
      needDispatch = true,
      resourceExhaustedStrategy = ResourceExhaustedStrategy.WAITING,
      timeout = submitJobRequest.resourceOptions.timeoutSeconds,
      sessionId = sessionId,
      sessionName = JobProcessorTypes.DeepSpeed.toString
    )
    ClusterResourceManager.submitResourceRequest(resourceApplication)
    var dispatchedProcessors = resourceApplication.getResult()
    logInfo(s"dispatchedProcessor: ${dispatchedProcessors.mkString("Array(", ", ", ")")}")

    //        smDao.register(ErSessionMeta(
    //          id = sessionId,
    //          processors = dispatchedProcessors.map(_._1),
    //          totalProcCount = worldSize,
    //          status = SessionStatus.NEW)
    //        )
    try {
      val registeredSessionMeta = smDao.getSession(submitJobRequest.sessionId)
      dispatchedProcessors = dispatchedProcessors.zip(registeredSessionMeta.processors).map {
        case ((processor, node), registeredProcessor) =>
          (processor.copy(id = registeredProcessor.id), node)
      }
      // register ranks
      val ranks = dispatchedProcessors.map { case (processor, node) =>
        (processor.id, node.id, processor.options.get("localRank").toInt, processor.options.get("globalRank").toInt)
      }
      smDao.registerRanks(sessionId, ranks.map(_._1), ranks.map(_._2), ranks.map(_._3), ranks.map(_._4))

      // start containers
      val groupedProcessors = dispatchedProcessors.groupBy(_._2)
      val crossSize = groupedProcessors.size

      groupedProcessors.zipWithIndex.par.foreach { case ((node, nodeAndProcessors), crossRank) =>
        val processors = nodeAndProcessors.map(_._1.copy(sessionId = submitJobRequest.sessionId))
        val nodeManagerClient = new NodeManagerClient(node.endpoint)
        // ClusterResourceManager.preAllocateResource(processors)

        val localSize = processors.length
        nodeManagerClient.startJobContainers(
          StartDeepspeedContainerRequest(
            sessionId = sessionId,
            name = submitJobRequest.name,
            commandArguments = submitJobRequest.commandArguments,
            environmentVariables = submitJobRequest.environmentVariables,
            files = submitJobRequest.files,
            zippedFiles = submitJobRequest.zippedFiles,
            deepspeedConfigs = processors.map { p =>
              p.id -> DeepspeedContainerConfig(
                cudaVisibleDevices = p.options.getOrDefault("cudaVisibleDevices", "0,1").split(",").map(_.toInt),
                worldSize = worldSize,
                crossRank = crossRank,
                crossSize = crossSize,
                localSize = localSize,
                localRank = p.options.get("localRank").toInt,
                rank = p.options.get("globalRank").toInt,
                storePrefix = sessionId
              )
            }(collection.breakOut),
            options = submitJobRequest.options))
      }

      // wait for containers to start, throw exception if timeout
      val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
      val activeProcessors = waitSubmittedContainers(sessionId, worldSize, startTimeout)

      // update options since some options are loss in db
      val idToOptions = dispatchedProcessors.map { case (processor, _) =>
        (processor.id, processor.options)
      }.toMap
      for (processor <- activeProcessors) {
        val options = idToOptions(processor.id)
        processor.options.putAll(options)
      }

      // active
      smDao.updateSessionStatus(sessionId = sessionId,
        status = SessionStatus.ACTIVE,
        required_old_status = Some(SessionStatus.NEW))
      SubmitJobResponse(sessionId, activeProcessors)

    }
    catch {
      case e: Exception =>
        smDao.updateSessionStatus(sessionId, SessionStatus.ERROR)
        throw e
    }
  }

  def killJob(sessionId: String, isTimeout: Boolean): Unit = {
    logInfo(s"killJob $sessionId")
    if (!smDao.existSession(sessionId)) {
      return
    }
    val sessionMeta = smDao.getSession(sessionId)
    if (StringUtils.equalsAny(sessionMeta.status, SessionStatus.KILLED, SessionStatus.CLOSED, SessionStatus.ERROR)) {
      return
    }
    val serverNodeCrudOperator = new ServerNodeCrudOperator()
    val nodeAndProcessors = sessionMeta.processors
      .groupBy(p => p.serverNodeId)
      .map { case (nodeId, processors) =>
        (serverNodeCrudOperator.getServerNode(ErServerNode(id = nodeId)), processors)
      }

    nodeAndProcessors.par.foreach { case (node, processors) =>
      try {
        new NodeManagerClient(node.endpoint)
          .killJobContainers(KillContainersRequest(sessionId = sessionId, processors = processors))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
    if (isTimeout) {
      smDao.updateSessionStatus(sessionId = sessionId, status = SessionStatus.NEW_TIMEOUT)
    } else {
      smDao.updateSessionStatus(sessionId = sessionId, status = SessionStatus.KILLED)
    }
  }
}
