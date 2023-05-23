package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant.{CoreConfKeys, ProcessorStatus, ResourceExhaustedStrategy, SessionConfKeys}
import com.webank.eggroll.core.containers.JobProcessorTypes
import com.webank.eggroll.core.deepspeed.job.meta.{ResourceOptions, SubmitJobRequest}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.StaticErConf

import java.io.File

object TestAssets {
  val proc1: ErProcessor = ErProcessor(serverNodeId = 1, status = ProcessorStatus.NEW)
  val sessionMeta1: ErSessionMeta = ErSessionMeta(
    id = "sid_reg1", tag = "tag1",
    options = Map("a" -> "b", "c" -> "d"), processors = Array(proc1))

  val cc1: CommandClient = new CommandClient(ErEndpoint("localhost:4670"))
  val sm1: SessionManager = cc1.proxy[SessionManager]

  val getOrCreateSessionMeta = ErSessionMeta(id = "testing_reg"+System.currentTimeMillis(), options = Map(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE -> "2"))

//  val pythonExec: String = conf.getString("eggroll.container.python.exec")
//  val scriptPath: String = conf.getString("eggroll.container.script.path")

//  var submitJobMeta= ErJobMeta(id="test_deepspeed"+System.currentTimeMillis(),
//    jobType = JobProcessorTypes.DeepSpeed.toString,
//    worldSize=2,options = Map("eggroll.container.python.exec"->"python",
//      "eggroll.container.script.path"->"/data/projects/myeggroll/python/eggroll/mock.py"))
//val DEEPSPEED_PYTHON_EXEC = "eggroll.container.deepspeed.python.exec"
//  val DEEPSPEED_SCRIPT_PATH = "eggroll.container.deepspeed.script.path"
  var submitJobMeta= SubmitJobRequest(sessionId="test_deepspeed"+System.currentTimeMillis(),
    jobType = JobProcessorTypes.DeepSpeed.toString,
    worldSize=2,
  resourceOptions  = ResourceOptions(timeoutSeconds=3000,resourceExhaustedStrategy=ResourceExhaustedStrategy.WAITING),
    options = Map("eggroll.container.deepspeed.python.exec"->"python",
      "eggroll.container.deepspeed.script.path"->"/data/projects/myeggroll/python/eggroll/mock.py"))


  def initConf(): Unit = {
    val confFile = new File("../../conf/eggroll.properties.local")
    println(confFile.getAbsolutePath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, "testing")
    StaticErConf.addProperties(confFile.getAbsolutePath)
  }
}
