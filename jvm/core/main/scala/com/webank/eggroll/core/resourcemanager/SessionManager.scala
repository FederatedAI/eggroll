package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

// RollObjects talk to SessionManager only.
trait SessionManager {
  def heartbeat(proc: ErProcessor): ErProcessor

  def getSessionMain(sessionId: String): ErSessionMeta
  /**
   * get or create session
   * @param sessionMeta session main and options
   * @return session main and options and processors
   */
  def getOrCreateSession(sessionMeta: ErSessionMeta): ErSessionMeta

  /**
   * get session detail
   * @param sessionMeta contains session id
   * @return session main and options and processors
   */
  def getSession(sessionMeta: ErSessionMeta): ErSessionMeta

  /**
   * register session without boot processors
   * @param sessionMeta contains session main and options and processors
   * @return
   */
  def registerSession(sessionMeta: ErSessionMeta): ErSessionMeta

  def stopSession(sessionMeta: ErSessionMeta): ErSessionMeta

  def killSession(sessionMeta: ErSessionMeta): ErSessionMeta

  def killSession(sessionMeta: ErSessionMeta, afterState: String): ErSessionMeta

  def killAllSessions(sessionMeta: ErSessionMeta): ErSessionMeta
}

class SessionManagerService extends SessionManager with Logging {
  private val smDao = new SessionMetaDao
  def heartbeat(proc: ErProcessor): ErProcessor = {
    smDao.updateProcessor(proc)
    proc
  }

  def getSessionMain(sessionId: String): ErSessionMeta = {
    smDao.getSessionMain(sessionId)
  }
  /**
   * get or create session
   * @param sessionMeta session main and options
   * @return session main and options and processors
   */
  def getOrCreateSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    val sessionId = sessionMeta.id
    if (smDao.existSession(sessionId)) {
      var result = smDao.getSession(sessionId)

      if (result != null) {
        if (result.status.equals(SessionStatus.ACTIVE)) {
          return result
        } else {
          return this.getSession(sessionMeta)
        }
      }
    }
    // 0. generate a simple processors -> server plan, and fill sessionMeta.processors
    // 1. class NodeManager.startContainers
    // 2. query session_main's active_proc_count , wait all processor heart beats.

    val healthyNodeExample = ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
    val serverNodeCrudOperator = new ServerNodeCrudOperator()

    val healthyCluster = serverNodeCrudOperator.getServerNodes(healthyNodeExample)

    val serverNodes = healthyCluster.serverNodes
    val serverNodesToHost = mutable.Map[Long, String]()
    serverNodes.foreach(n => serverNodesToHost += (n.id -> n.endpoint.host))

    val eggsPerNode = sessionMeta.options.getOrElse(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, "1")).toInt
    // TODO:1: use constants instead of processor_types,processor_plan,uniform
    val processorPlan =
      if(sessionMeta.options.contains("processor_types")) {
        val processor_types = sessionMeta.options("processor_types").split(",")
        processor_types.flatMap { pType =>
          val planStrategy = sessionMeta.options("processor_plan." + pType)
          require(planStrategy == "uniform", s"unsupported:${planStrategy}")
          serverNodes.flatMap(n =>
            (0 until eggsPerNode).map(_ => ErProcessor(
              serverNodeId = n.id,
              processorType = pType,
              status = ProcessorStatus.NEW)
            )
          )
        }
      } else {
        serverNodes.flatMap(n => (0 until eggsPerNode).map(_ => ErProcessor(
          serverNodeId = n.id,
          processorType = ProcessorTypes.EGG_PAIR,
          commandEndpoint = ErEndpoint(serverNodesToHost(n.id), 0),
          status = ProcessorStatus.NEW)))
      }

    val expectedProcessorsCount = processorPlan.length
    val sessionMetaWithProcessors = sessionMeta.copy(
      processors = processorPlan,
      totalProcCount = expectedProcessorsCount,
      activeProcCount = 0,
      status = SessionStatus.NEW)

    smDao.register(sessionMetaWithProcessors)
    // TODO:0: record session failure in database if session start is not successful, and returns error session
    val registeredSessionMeta = smDao.getSession(sessionMeta.id)

    serverNodes.par.foreach(n => {
      // TODO:1: add new params?
      val newSessionMeta = registeredSessionMeta.copy(
        options = registeredSessionMeta.options ++ Map(ResourceManagerConfKeys.SERVER_NODE_ID -> n.id.toString))
      val nodeManagerClient = new NodeManagerClient(
        ErEndpoint(host = n.endpoint.host,
          port = StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1)))
      nodeManagerClient.startContainers(newSessionMeta)
    })

    val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
    var isStarted = false
    breakable {
      while (System.currentTimeMillis() <= startTimeout) {
        val cur = getSessionMain(sessionId)
        if (cur.activeProcCount < expectedProcessorsCount) {
          Thread.sleep(100)
        } else {
          isStarted = true
          break
        }
      }
    }

    if (!isStarted) {
      val curDetails = smDao.getSession(sessionId)

      // last chance to check
      if (curDetails.activeProcCount < expectedProcessorsCount) {
        val actives = ListBuffer[Long]()
        val inactives = ListBuffer[Long]()
        val activesPerNode = mutable.Map[String, Int]()
        val inactivesToNode = mutable.Map[Long, String]()

        serverNodes.foreach(n => activesPerNode += (n.endpoint.host -> 0))

        curDetails.processors.foreach(p => {
          if (p.status.equals(ProcessorStatus.RUNNING)) {
            actives += p.id
            activesPerNode(p.commandEndpoint.host) += 1
          } else {
            inactives += p.id
            inactivesToNode += (p.id -> serverNodesToHost(p.serverNodeId))
          }
        })

        killSession(sessionMeta = curDetails, afterState = SessionStatus.ERROR)

        val builder = new mutable.StringBuilder()
        builder.append(s"unable to start all processors for session id: '${sessionId}'. ")
          .append(s"Please check corresponding bootstrap logs at '${CoreConfKeys.EGGROLL_LOGS_DIR.get()}/${sessionId}' to check the reasons. Details:\n")
          .append("=================\n")
          .append(s"total processors: ${curDetails.totalProcCount}, \n")
          .append(s"started count: ${curDetails.activeProcCount}, \n")
          .append(s"not started count: ${curDetails.totalProcCount - curDetails.activeProcCount}, \n")
          .append(s"current active processors per node: ${activesPerNode}, \n")
          .append(s"not started processors and their nodes: ${inactivesToNode}")
        val exception = new ErSessionException(builder.toString())
        throw exception
      }
    }

    // todo:1: update selective
    smDao.updateSessionMain(registeredSessionMeta.copy(
      status = SessionStatus.ACTIVE, activeProcCount = expectedProcessorsCount))
    getSession(sessionMeta)
  }

  /**
   * get session detail
   * @param sessionMeta contains session id
   * @return session main and options and processors
   */
  def getSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    logDebug(s"SESSION getSession: ${sessionMeta}")
    var result: ErSessionMeta = null
    val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
    // todo:1: use retry framework
    breakable {
      while (System.currentTimeMillis() <= startTimeout) {
        result = smDao.getSession(sessionMeta.id)
        if (result != null && !result.status.equals(SessionStatus.NEW) && !StringUtils.isBlank(result.id)) {
          break()
        } else {
          Thread.sleep(100)
        }
      }
    }

    result
  }

  /**
   * register session without boot processors
   * @param sessionMeta contains session main and options and processors
   * @return
   */
  def registerSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    // TODO:0: + active processor count and expected ones; session status 'active' from client
    smDao.register(sessionMeta.copy(status = SessionStatus.ACTIVE,
      totalProcCount = sessionMeta.processors.length,
      activeProcCount = sessionMeta.processors.length))
    // generated id
    smDao.getSession(sessionMeta.id)
  }

  override def stopSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    val sessionId = sessionMeta.id
    logDebug(s"stopping session: ${sessionId}")
    if (!smDao.existSession(sessionId)) {
      return null
    }

    val dbSessionMeta = smDao.getSession(sessionId)

    if (dbSessionMeta.status.equals(SessionStatus.CLOSED)) {
      return dbSessionMeta
    }

    val sessionHosts = mutable.Set[String]()
    dbSessionMeta.processors.foreach(p => {
      if (p != null && p.commandEndpoint != null) sessionHosts += p.commandEndpoint.host
    })

    val serverNodeCrudOperator = new ServerNodeCrudOperator()
    val sessionServerNodes = serverNodeCrudOperator.getServerClusterByHosts(sessionHosts.toList.asJava).serverNodes

    logDebug(s"stopping session. session id: ${sessionId}, hosts: ${sessionHosts}, nodes: ${sessionServerNodes.map(n => n.id).toList}")

    if (sessionServerNodes.isEmpty) {
      throw new IllegalStateException(s"stopping a session with empty nodes. session id: ${sessionId}")
    }
    sessionServerNodes.foreach(n => {
      // TODO:1: add new params?
      val newSessionMeta = dbSessionMeta.copy(
        options = dbSessionMeta.options ++ Map(ResourceManagerConfKeys.SERVER_NODE_ID -> n.id.toString))
      val nodeManagerClient = new NodeManagerClient(
        ErEndpoint(host = n.endpoint.host,
          port = StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1)))
      nodeManagerClient.stopContainers(newSessionMeta)
    })

    val stopTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_STOP_TIMEOUT_MS.get().toLong
    var isStopped = false

    breakable {
      while (System.currentTimeMillis() <= stopTimeout) {
        val cur = getSessionMain(sessionId)
        if (cur.activeProcCount > 0) {
          Thread.sleep(100)
        } else {
          isStopped = true
          break
        }
      }
    }
    if (!isStopped) throw new IllegalStateException("unable to stop all processors")

    // todo:1: update selective
    val stoppedSessionMain = dbSessionMeta.copy(activeProcCount = 0, status = SessionStatus.CLOSED)
    smDao.updateSessionMain(stoppedSessionMain)
    stoppedSessionMain
  }

  override def killSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    killSession(sessionMeta, SessionStatus.KILLED)
  }

  override def killSession(sessionMeta: ErSessionMeta, afterState: String): ErSessionMeta = {
    val sessionId = sessionMeta.id
    if (!smDao.existSession(sessionId)) {
      return null
    }

    val dbSessionMeta = smDao.getSession(sessionId)

    if (StringUtils.equalsAny(dbSessionMeta.status, SessionStatus.KILLED, SessionStatus.CLOSED, SessionStatus.ERROR)) {
      return dbSessionMeta
    }

    val sessionHosts = dbSessionMeta.processors.map(p => p.commandEndpoint.host).toSet

    val serverNodeCrudOperator = new ServerNodeCrudOperator()
    val sessionServerNodes = serverNodeCrudOperator.getServerClusterByHosts(sessionHosts.toList.asJava).serverNodes

    sessionServerNodes.par.foreach(n => {
      // TODO:1: add new params?
      val newSessionMeta = dbSessionMeta.copy(
        options = dbSessionMeta.options ++ Map(ResourceManagerConfKeys.SERVER_NODE_ID -> n.id.toString))
      val nodeManagerClient = new NodeManagerClient(
        ErEndpoint(host = n.endpoint.host,
          port = StaticErConf.getInt(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, -1)))
      nodeManagerClient.killContainers(newSessionMeta)
    })

    // todo:1: update selective
    smDao.updateSessionMain(dbSessionMeta.copy(activeProcCount = 0, status = afterState))
    getSession(dbSessionMeta)
  }

  // todo:1: return value
  override def killAllSessions(sessionMeta: ErSessionMeta): ErSessionMeta = {
    val sessionStatusStub = Array(ErSessionMeta(status = SessionStatus.NEW), ErSessionMeta(status = SessionStatus.ACTIVE))
    val sessionsToKill = sessionStatusStub.flatMap(s => smDao.getSessionMains(s))

    sessionsToKill.par.map(s => killSession(s))

    ErSessionMeta()
  }
}