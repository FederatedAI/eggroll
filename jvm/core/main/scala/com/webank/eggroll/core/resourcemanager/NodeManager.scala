package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.ErSessionMeta

trait NodeManager {
  def startContainers(sessionMeta: ErSessionMeta):Unit
}

class NodeManagerService extends NodeManager {
  override def startContainers(sessionMeta: ErSessionMeta):Unit = ???
}
