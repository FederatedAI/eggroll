package com.webank.eggroll.core.resourcemanager

import java.io.File

import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant.{CoreConfKeys, ProcessorStatus, SessionConfKeys}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.StaticErConf

object TestAssets {
  val proc1: ErProcessor = ErProcessor(serverNodeId = 1, status = ProcessorStatus.NEW)
  val sessionMeta1:ErSessionMeta = ErSessionMeta(
    id="sid_reg1", tag = "tag1",
    options = Map("a"->"b","c"->"d"), processors = Array(proc1))

  val cc1:CommandClient = new CommandClient(ErEndpoint("localhost:4670"))
  val sm1:SessionManager = cc1.proxy[SessionManager]

  val getOrCreateSessionMeta = ErSessionMeta(id = "testing_reg", options = Map(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE -> "2"))

  def initConf(): Unit = {
    val confFile = new File("../../conf/eggroll.properties.local")
    println(confFile.getAbsolutePath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, "testing")
    StaticErConf.addProperties(confFile.getAbsolutePath)
  }
}
