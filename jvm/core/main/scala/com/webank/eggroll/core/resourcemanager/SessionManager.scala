package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}

// RollObjects talk to SessionManager only.
class SessionManager {
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
