package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ResourceManagerConfKeys
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.RuntimeErConf

trait NodeManager {
  def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def stopContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def killContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def heartbeat(processor: ErProcessor): ErProcessor
}

class NodeManagerService extends NodeManager {
  override def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    operateContainers(sessionMeta, "start")
  }

  override def stopContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    operateContainers(sessionMeta, "stop")
  }

  override def killContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    operateContainers(sessionMeta, "kill")
  }

  private def operateContainers(sessionMeta: ErSessionMeta, opType: String): ErSessionMeta = {
    val processorPlan = sessionMeta.processors

    val runtimeConf = new RuntimeErConf(sessionMeta)
    val myServerNodeId = runtimeConf.getLong(ResourceManagerConfKeys.SERVER_NODE_ID, -1)

    val result = processorPlan.par.map(p => {
      if (p.serverNodeId == myServerNodeId) {
        val container = new Container(runtimeConf, p.processorType, p.id)
        opType match {
          case "start" => container.start()
          case "stop" => container.stop()
          case "kill" => container.kill()
          case _ => throw new IllegalArgumentException(s"op not supported: '${opType}'")
        }
      }
    })

    sessionMeta
  }

  override def heartbeat(processor: ErProcessor): ErProcessor = {
    null
  }
}
