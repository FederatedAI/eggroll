package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErSessionMeta}

object TestAssets {
  val proc1: ErProcessor = ErProcessor(serverNodeId = 1, status = RmConst.PROC_NEW)
  val sessionMeta1:ErSessionMeta = ErSessionMeta(
    id="sid_reg1", tag = "tag1",
    options = Map("a"->"b","c"->"d"), processors = List(proc1))

  val cc1:CommandClient = new CommandClient(ErEndpoint("localhost:4671"))
  val sm1:SessionManager = cc1.proxy[SessionManager]
}
