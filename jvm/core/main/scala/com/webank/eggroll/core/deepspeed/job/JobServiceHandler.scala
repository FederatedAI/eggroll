package com.webank.eggroll.core.deepspeed.job

import com.google.gson.Gson
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
import com.webank.eggroll.core.resourcemanager.{ClusterResourceManager, SessionMetaDao}
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import java.util
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object JobServiceHandler extends Logging {
  private lazy val smDao = new SessionMetaDao
  private lazy val nodeDao = new  ServerNodeCrudOperator;

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

  def prepareJobDownload (prepareRequest: PrepareJobDownloadRequest):PrepareJobDownloadResponse = {
    val sessionId = prepareRequest.sessionId
    logInfo(f"download job request, sessionId: ${sessionId}")
    val contentType = prepareRequest.contentType
    val compressMethod = prepareRequest.compressMethod
    var contentMap =  new util.HashMap[Any,Any]()
    var processors =  ArrayBuffer[ErProcessor]()

    val nodeIdMeta = smDao.getRanks(sessionId).flatMap { case (containerId, nodeId, globalRank, localRank) =>
      val index = if (prepareRequest.ranks.isEmpty) globalRank else prepareRequest.ranks.indexOf(globalRank)
      if (index >= 0) {
        Some(nodeId, containerId, globalRank, localRank, index)
      } else {
        None
      }
    }.groupBy(_._1)


    nodeIdMeta .map(t=>{
      var options = new  util.HashMap[String,String]()
      var serverNodeIndb = nodeDao.getServerNode(ErServerNode(id = t._1))
      if(serverNodeIndb != null) {
        options.put("ip",serverNodeIndb.endpoint.host)
        options.put("port",serverNodeIndb.endpoint.port.toString)
           contentMap.put(serverNodeIndb.id.toString,t._2.map(e=>{Array(e._1,e._2,e._3,e._4,e._5)}))

           processors.append(ErProcessor(sessionId = prepareRequest.sessionId,
             serverNodeId = t._1,
             processorType = ProcessorTypes.EGG_PAIR,
             name = "DS-DOWNLOAD",
             status = ProcessorStatus.NEW,
             options =  options

           ))
        logInfo("===================append processor ")
      }
    })



    logInfo(s"========processor size = ${processors.length}")
    if(processors.length==0){
      throw  new ErSessionException(s"can not find download rank info for session ${sessionId} ")
    }

    var  newSession = ClusterResourceManager.submitJodDownload(ResourceApplication(sessionId = "DS-DOWNLOAD-"+System.currentTimeMillis()+"-"+scala.util.Random.nextInt(100).toString,
      processors=processors.toArray
    ))

    if(newSession.status!=SessionStatus.ACTIVE){
      throw  new  ErSessionException("session status is "+newSession.status)
    }
    logInfo(s"===========xxxxx ${contentMap} ")
    newSession.processors.map(p=>{
      logInfo(s"ppppppppppppp  ${p}")
      if(contentMap.containsKey(p.serverNodeId.toString)){
        logInfo(s"iiiiiiiiiiiiiiiiii")
        contentMap.put(     p.transferEndpoint.toString,contentMap.get(p.serverNodeId.toString))
        logInfo(s"hhhhhhhhhhhhhhhhh ${contentMap}")
        contentMap.remove(p.serverNodeId.toString)
      }else{
        logInfo(s"================= cannot found ${p.serverNodeId}")
      }
    })


    var  gson= new Gson()
    logInfo(s"============${gson.toJson(contentMap)}");
    PrepareJobDownloadResponse(sessionId = newSession.id, content = gson.toJson(contentMap))
  }





  def handleJobDownload(downloadJobRequest: DownloadJobRequest): DownloadJobResponse = {
    val sessionId = downloadJobRequest.sessionId
    logInfo(f"download job request, sessionId: ${sessionId}")
    val contentType = downloadJobRequest.contentType
    val compressMethod = downloadJobRequest.compressMethod

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
      logInfo(f"download from node, sessionId: ${sessionId}, node: ${node}, indexes: ${indexes.mkString(",")}")
      val containerContents = {
        try {
          new NodeManagerClient(node.endpoint).downloadContainers(
            DownloadContainersRequest(
              sessionId = sessionId,
              containerIds = ranks.map(_._2),
              compressMethod = compressMethod,
              compressLevel = downloadJobRequest.compressLevel,
              contentType = contentType
            )).containerContents
        } catch {
          case e: Exception =>
            e.printStackTrace()
            ranks.map(r => ContainerContent(
              containerId = r._2,
              content = Array.empty,
              compressMethod = downloadJobRequest.compressMethod))
        }
      }
      logInfo(f"download from node done, sessionId: ${sessionId}, node: ${node}, indexes: ${indexes.mkString(",")}")
      indexes.zip(containerContents)
    }.toArray.sortBy(_._1).map(_._2)
    logInfo(f"download job request done, sessionId: ${sessionId}")
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
      val session = smDao.getSessionMain(sessionId)
      val activeCount = session.activeProcCount
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
      sortByResourceType = ResourceTypes.VGPU_CORE,
      processors = prepareProcessors,
      resourceExhaustedStrategy = ResourceExhaustedStrategy.WAITING,
      timeout = submitJobRequest.resourceOptions.timeoutSeconds * 1000,
      sessionId = sessionId,
      sessionName = JobProcessorTypes.DeepSpeed.toString
    )
    logInfo(s"submitting resource request: ${resourceApplication}, ${resourceApplication.hashCode()}")
    ClusterResourceManager.submitResourceRequest(resourceApplication)
    var dispatchedProcessors = resourceApplication.getResult()
    logInfo(s"submitted resource request: ${resourceApplication}, ${resourceApplication.hashCode()}")
    logInfo(s"dispatchedProcessor: ${dispatchedProcessors.mkString("Array(", ", ", ")")}")


    try {
      //锁不能移到分配资源之前，会造成死锁
      ClusterResourceManager.lockSession(sessionId)
      if(!ClusterResourceManager.killJobMap.contains(sessionId)) {
      val registeredSessionMeta = smDao.getSession(submitJobRequest.sessionId)
      dispatchedProcessors = dispatchedProcessors.zip(registeredSessionMeta.processors).map {
        case ((processor, node), registeredProcessor) =>
          (processor.copy(id = registeredProcessor.id), node)
      }

      val deepspeedConfigsWithNode = {
        var globalRank = 0 // increment by 1 for each processor
        var crossSize = 0 // number of nodes
        var crossRank = 0 // increment by 1 for each node
        val configs = ArrayBuffer[(Long, ErServerNode, DeepspeedContainerConfig)]()
        dispatchedProcessors.groupBy(_._2).foreach { case (node, processorAndNodeArray) =>
          crossSize += 1
          val localSize = processorAndNodeArray.length
          val cudaVisibleDevices = processorAndNodeArray.map { case (p, _) =>
            val devicesForProcessor = p.options.getOrDefault("cudaVisibleDevices", "-1").split(",").map(_.toInt)
            if (devicesForProcessor.length < 1 || devicesForProcessor.exists(_ < 0)) {
              throw new IllegalArgumentException(s"cudaVisibleDevices is not set or invalid: ${p.options.get("cudaVisibleDevices")}")
            }
            devicesForProcessor
          }.reduce(_ ++ _)
          if (cudaVisibleDevices.length != cudaVisibleDevices.distinct.length) {
            throw new IllegalArgumentException(s"duplicate cudaVisibleDevices: ${cudaVisibleDevices.mkString(",")}")
          }
          var localRank = 0
          processorAndNodeArray.foreach { case (p, _) =>
            configs.append((p.id, node, DeepspeedContainerConfig(
              cudaVisibleDevices = cudaVisibleDevices,
              worldSize = worldSize,
              crossRank = crossRank,
              crossSize = crossSize,
              localSize = localSize,
              localRank = localRank,
              rank = globalRank,
              storePrefix = sessionId
            )))
            localRank += 1
            globalRank += 1
          }
          crossRank += 1
        }
        configs.toArray
      }

      // register ranks
      val ranks = deepspeedConfigsWithNode.map { case (processorId, node, config) =>
        (processorId, node.id, config.localRank, config.rank)
      }
      smDao.registerRanks(sessionId, ranks.map(_._1), ranks.map(_._2), ranks.map(_._3), ranks.map(_._4))

      // start containers


        deepspeedConfigsWithNode.groupBy(_._2).par.foreach { case (node, nodeAndConfigs) =>
          val nodeManagerClient = new NodeManagerClient(node.endpoint)
          nodeManagerClient.startJobContainers(
            StartDeepspeedContainerRequest(
              sessionId = sessionId,
              name = submitJobRequest.name,
              commandArguments = submitJobRequest.commandArguments,
              environmentVariables = submitJobRequest.environmentVariables,
              files = submitJobRequest.files,
              zippedFiles = submitJobRequest.zippedFiles,
              deepspeedConfigs = nodeAndConfigs.map { case (containerId, _, config) => containerId -> config }(collection.breakOut),
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
      }else{
        logError(s"kill session ${sessionId} request was found")
          throw new ErSessionException(s"kill session ${sessionId} request was found")
      }
    }
    catch {
      case e: Exception =>
        killJob(sessionId, isTimeout = false)
        throw e
    }finally {
       ClusterResourceManager.unlockSession(sessionId)
    }
  }

  def killJob(sessionId: String, isTimeout: Boolean): Unit = {
    logInfo(s"killing job $sessionId")
    try {
      ClusterResourceManager.lockSession(sessionId)
      ClusterResourceManager.killJobMap.put(sessionId,System.currentTimeMillis())
      if (!smDao.existSession(sessionId)) {
        return
      }
    val sessionMeta = smDao.getSession(sessionId)
    if (StringUtils.equalsAny(sessionMeta.status, SessionStatus.KILLED, SessionStatus.CLOSED, SessionStatus.ERROR)) {
      return
    }
    val serverNodeCrudOperator = new ServerNodeCrudOperator()
     var nodeAndProcessors = sessionMeta.processors
      .groupBy(p => p.serverNodeId)
      .map { case (nodeId, processors) =>
        (serverNodeCrudOperator.getServerNode(ErServerNode(id = nodeId)), processors)
      }
      nodeAndProcessors.par.foreach { case (node, processors) =>
        try {
          new NodeManagerClient(node.endpoint)
            .killJobContainers(KillContainersRequest(sessionId = sessionId, containers = processors.map(_.id)))
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    smDao.updateSessionMain(sessionMeta.copy(status = SessionStatus.ERROR), afterCall = defaultSessionCallback)
    }finally {
      ClusterResourceManager.unlockSession(sessionId)
    }
  }
}
