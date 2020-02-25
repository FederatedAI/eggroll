package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging

import scala.collection.JavaConverters._
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
    val eggsPerNode = sessionMeta.options.getOrElse(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE, "1").toInt
    val processorPlan = Array(ErProcessor(
      serverNodeId = serverNodes.head.id,
      processorType = ProcessorTypes.ROLL_PAIR_MASTER,
      status = ProcessorStatus.NEW)) ++
      serverNodes.flatMap(n => (0 until eggsPerNode).map(_ => ErProcessor(
        serverNodeId = n.id,
        processorType = ProcessorTypes.EGG_PAIR,
        status = ProcessorStatus.NEW)))
    val expectedProcessorsCount = 1 + healthyCluster.serverNodes.length * eggsPerNode
    val sessionMetaWithProcessors = sessionMeta.copy(processors = processorPlan, activeProcCount = 0, status = SessionStatus.NEW)

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

    val maxRetries = 200

    breakable {
      (0 until maxRetries).foreach(i => {
        val cur = getSessionMain(sessionId)
        if (cur.activeProcCount < expectedProcessorsCount) Thread.sleep(100) else break
        if (i == maxRetries - 1) throw new IllegalStateException("unable to start all processors")
      })
    }

    // todo:1: update selective
    smDao.updateSessionMain(registeredSessionMeta.copy(status = SessionStatus.ACTIVE, activeProcCount = expectedProcessorsCount))
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
    // todo:1: use retry framework
    breakable {
      var retries = 0
      while (retries < 200) {
        result = smDao.getSession(sessionMeta.id)
        if (result != null && result.status.equals(SessionStatus.ACTIVE)) {
          break()
        } else if (result.status.equals(SessionStatus.CLOSED)) {
          throw new IllegalStateException(s"Session has already stopped when getting: ${result}")
        } else {
          Thread.sleep(100)
        }
      }
    }

    if (result.status.equals(SessionStatus.ACTIVE)) {
      result
    } else {
      throw new RuntimeException(s"Failed to get session ${sessionMeta.id} after retries")
    }
  }

  /**
   * register session without boot processors
   * @param sessionMeta contains session main and options and processors
   * @return
   */
  def registerSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    // TODO:0: + active processor count and expected ones; session status 'active' from client
    smDao.register(sessionMeta.copy(status = SessionStatus.ACTIVE, activeProcCount = sessionMeta.processors.length))
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

    val sessionHosts = dbSessionMeta.processors.map(p => p.commandEndpoint.host).toSet

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

    val maxRetries = 200

    breakable {
      (0 until maxRetries).foreach(i => {
        val cur = getSessionMain(sessionId)
        if (cur.activeProcCount > 0) Thread.sleep(100) else break
        if (i == maxRetries - 1) throw new IllegalStateException("unable to stop all processors")
      })
    }

    // todo:1: update selective
    val stoppedSessionMain = dbSessionMeta.copy(activeProcCount = 0, status = SessionStatus.CLOSED)
    smDao.updateSessionMain(stoppedSessionMain)
    stoppedSessionMain
  }

  override def killSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    val sessionId = sessionMeta.id
    if (!smDao.existSession(sessionId)) {
      return null
    }

    val dbSessionMeta = smDao.getSession(sessionId)

    if (dbSessionMeta.status.equals(SessionStatus.KILLED) || dbSessionMeta.status.equals(SessionStatus.CLOSED)) {
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
    smDao.updateSessionMain(dbSessionMeta.copy(activeProcCount = 0, status = SessionStatus.KILLED))
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