package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ResourceManagerConfKeys
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.RuntimeErConf

trait NodeManager {
  def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta
  def heartbeat(processor: ErProcessor): ErProcessor
}

class NodeManagerService extends NodeManager {
  override def startContainers(sessionMeta: ErSessionMeta): ErSessionMeta = {
    val processorPlan = sessionMeta.processors

    val runtimeConf = new RuntimeErConf(sessionMeta)
    val myServerNodeId = runtimeConf.getLong(ResourceManagerConfKeys.SERVER_NODE_ID, -1)
    val result = processorPlan.par.map(p => {
      if (p.serverNodeId == myServerNodeId) {
        val container = new Container(runtimeConf, p.processorType, p.id)
        container.start()
      }
    })

    sessionMeta
  }

  override def heartbeat(processor: ErProcessor): ErProcessor = {
    null
  }
}
