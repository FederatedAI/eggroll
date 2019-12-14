package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}

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
    if (smDao.exitsSession(sessionMeta.id)) {
      return smDao.getSession(sessionMeta.id)
    }
    // TODO:0: dispatch processor
    // 0. generate a simple processors -> server plan, and fill sessionMeta.processors
    // 1. class NodeManager.bootSessionProcessors
    // 2. query session_main's active_proc_count , wait all processor heart beats.
    register(sessionMeta)
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