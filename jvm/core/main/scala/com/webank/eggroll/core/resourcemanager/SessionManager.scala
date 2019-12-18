package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.clustermanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.{ErProcessor, ErServerNode, ErSessionMeta}

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
  def register(sessionMeta: ErSessionMeta): ErSessionMeta
}

class SessionManagerService extends SessionManager {
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
    if (smDao.existSession(sessionMeta.id)) {
      return smDao.getSession(sessionMeta.id)
    }
    // TODO:0: dispatch processor
    // 0. generate a simple processors -> server plan, and fill sessionMeta.processors
    // 1. class NodeManager.startContainers
    // 2. query session_main's active_proc_count , wait all processor heart beats.

    val healthyNodeExample = ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
    val serverNodeCrudOperator = new ServerNodeCrudOperator()

    val healthyCluster = serverNodeCrudOperator.getServerNodes(healthyNodeExample)

    val serverNodes = healthyCluster.serverNodes
    val eggsPerNode = sessionMeta.options.getOrElse(SessionConfKeys.CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE, "1").toInt
    val processorPlan = Array(ErProcessor(
      serverNodeId = serverNodes.head.id,
      processorType = ProcessorTypes.ROLL_PAIR_MASTER,
      status = ProcessorStatus.NEW)) ++
      serverNodes.flatMap(n => (0 until eggsPerNode).map(_ => ErProcessor(
        serverNodeId = n.id,
        processorType = ProcessorTypes.EGG_PAIR,
        status = ProcessorStatus.NEW)))
    val expectedProcessorsCount = 1 + healthyCluster.serverNodes.length * eggsPerNode
    val sessionMetaWithProcessors = sessionMeta.copy(processors = processorPlan, activeProcCount = expectedProcessorsCount)

    smDao.register(sessionMetaWithProcessors)

    val registeredSessionMeta = smDao.getSession(sessionMeta.id)

    serverNodes.par.foreach(n => {
      val nodeManagerClient = new NodeManagerClient(n.endpoint)
      nodeManagerClient.startContainers(registeredSessionMeta)
    })

    val sessionId = sessionMeta.id
    val maxRetries = 200

    breakable {
      (0 until maxRetries).foreach(i => {
        val cur = getSessionMain(sessionId)
        if (cur.activeProcCount < expectedProcessorsCount) Thread.sleep(100) else break
        if (i == maxRetries - 1) throw new IllegalStateException("unable to start all processors")
      })
    }

    getSession(sessionMeta)
  }

  /**
   * get session detail
   * @param sessionMeta contains session id
   * @return session main and options and processors
   */
  def getSession(sessionMeta: ErSessionMeta): ErSessionMeta = {
    smDao.getSession(sessionMeta.id)
  }

  /**
   * register session without boot processors
   * @param sessionMeta contains session main and options and processors
   * @return
   */
  def register(sessionMeta: ErSessionMeta): ErSessionMeta = {
    smDao.register(sessionMeta)
    // generated id
    smDao.getSession(sessionMeta.id)
  }
}