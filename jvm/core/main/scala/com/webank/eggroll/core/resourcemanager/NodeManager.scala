package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.ErSessionMeta

trait NodeManager {
  def bootSessionProcessors(sessionMeta: ErSessionMeta):Unit
}

class NodeManagerService extends NodeManager {
  override def bootSessionProcessors(sessionMeta: ErSessionMeta):Unit = ???
}
