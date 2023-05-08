package com.webank.eggroll.core.resourcemanager.job

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.ClusterResourceManager.ResourceApplication
import com.webank.eggroll.core.resourcemanager.job.ClusterManagerJobService.smDao
import com.webank.eggroll.core.resourcemanager.{ClusterManagerService, ClusterResourceManager, ProcessorEvent, ProcessorEventCallback, SessionMetaDao}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import java.util.concurrent.CountDownLatch
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}


object ClusterManagerJobService extends Logging {
  private val smDao = new SessionMetaDao
  def killJob(sessionId :String):Unit ={
    logInfo(s"receive killJob ${sessionId}");

    if (!smDao.existSession(sessionId)) {
      return null
    }
    val serverNodeCrudOperator = new ServerNodeCrudOperator()
    val dbSessionMeta = smDao.getSession(sessionId)
    if (StringUtils.equalsAny(dbSessionMeta.status, SessionStatus.KILLED, SessionStatus.CLOSED, SessionStatus.ERROR)) {
      return dbSessionMeta
    }
    val nodeProcessorMap  = dbSessionMeta.processors.groupBy(p=>p.serverNodeId)
      .map(e=>(serverNodeCrudOperator.getServerNode(ErServerNode(id=e._1)),e._2))
    nodeProcessorMap.par.foreach(n=>{
      try {
        val nodeManagerClient = new NodeManagerClient(ErEndpoint(host = n._1.endpoint.host, port = n._1.endpoint.port))
          nodeManagerClient.killJobContainers(ErJobMeta(id = sessionId, processors = n._2))
      }catch {
        case e :Exception =>  e.printStackTrace()
      }
    })

  }
  ClusterManagerService.registerProcessorCallback(ProcessorEventType.PROCESSOR_LOSS,new ProcessorEventCallback {
    override def callback(event: ProcessorEvent): Unit = {
      new Thread(()=>{
        killJob(event.erProcessor.sessionId)
      }).start()
    }
  })
}

class ClusterManagerJobService extends Logging {


  def submitJob(submitJobMeta: ErJobMeta): ErJobMeta = {
    JobProcessorTypes.fromString(submitJobMeta.jobType) match {
      case Some(JobProcessorTypes.DeepSpeed) =>
        val worldSize = submitJobMeta.worldSize

         //ClusterResourceManager.dispatchDeepSpeed(worldSize)
        var prepareProcessors : ArrayBuffer[ErProcessor]  = ArrayBuffer()
        for (index <- 0 until worldSize) {
          prepareProcessors+= ErProcessor(
//            serverNodeId = node.id,
            processorType = JobProcessorTypes.DeepSpeed.toString,
//            commandEndpoint = ErEndpoint(host, 0),
            status = ProcessorStatus.NEW,
//            options = Map(
//              "globalRank" -> globalRank.toString,
//              "localRank" -> localRank.toString
//            ).asJava,
            resources=Array(ErResource(resourceType = ResourceTypes.VGPU_CORE,allocated = 1,status= ResourceStatus.PRE_ALLOCATED))
          )
        }

        var resourceApplication = new ResourceApplication(processors = prepareProcessors.toArray,
          needDispatch = true,countDownLatch = new CountDownLatch(1))
        ClusterResourceManager.submitResourceRequest(resourceApplication)
        var dispatchedProcessors  =  resourceApplication.getResult()
        logInfo(s"dispatchedProcessors =========== ${dispatchedProcessors}")

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
