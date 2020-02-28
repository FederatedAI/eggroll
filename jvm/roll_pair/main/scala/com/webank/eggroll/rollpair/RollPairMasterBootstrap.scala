package com.webank.eggroll.rollpair

import java.io.File
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import _root_.io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import com.webank.eggroll.core.Bootstrap
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.{ErEndpoint, ErJob, ErProcessor}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang3.StringUtils

class RollPairMasterBootstrap extends Bootstrap with Logging {
  private var port = 0
  private var sessionId = "er_session_null"
  private var nodeManager = ""
  private var args: Array[String] = _
  private var cmd: CommandLine = null

  override def init(args: Array[String]): Unit = {
    this.args = args
    cmd = CommandArgsUtils.parseArgs(args)
    sessionId = cmd.getOptionValue('s')

    CommandRouter.register(serviceName = RollPair.ROLL_RUN_JOB_COMMAND.uriString,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPair.RUN_JOB)
  }

  def reportCM(sessionId: String, args: Array[String], myCommandPort: Int):Unit = {
    // todo:2: heartbeat service
    val portString = cmd.getOptionValue('p', "0")
    val clusterManager = cmd.getOptionValue("cluster-manager", "localhost:4670")
    val nodeManager = cmd.getOptionValue("node-manager", "localhost:9394")
    val serverNodeId = cmd.getOptionValue("server-node-id", "0").toLong
    val confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    val processorId = cmd.getOptionValue("processor-id", "0").toLong

    val clusterManagerClient = new ClusterManagerClient(ErEndpoint(clusterManager))

    val options = new ConcurrentHashMap[String, String]()
    options.put(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    val processName = ManagementFactory.getRuntimeMXBean.getName
    val pid = processName.split(StringConstants.AT, 2)(0).toInt
    val myself = ErProcessor(
      id = processorId,
      serverNodeId = serverNodeId,
      processorType = ProcessorTypes.ROLL_PAIR_MASTER,
      commandEndpoint = ErEndpoint("localhost", myCommandPort),
      transferEndpoint = ErEndpoint("localhost", myCommandPort),
      pid = pid,
      options = options,
      status = ProcessorStatus.RUNNING)
    logInfo("ready to heartbeat")
    clusterManagerClient.heartbeat(myself)

    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logInfo(s"*** roll pair master exit gracefully. sessionId: ${sessionId}, serverNodeId: ${serverNodeId}, processorId: ${processorId}, port: ${portString} ***")
        val terminatedSelf = myself.copy(status = ProcessorStatus.STOPPED)
        clusterManagerClient.heartbeat(terminatedSelf)
        this.interrupt()
      }
    })
  }
  override def start(): Unit = {
    val managerEndpoint = if (StringUtils.isBlank(nodeManager)) {
      ErEndpoint(host = "localhost", port = 9394)
    } else {
      val splittedManager = nodeManager.trim.split(StringConstants.COLON, 2)
      println(splittedManager)
      println(splittedManager.length)

      val managerHost = if (splittedManager.length == 1) "localhost" else splittedManager(0)
      val managerPort = if (splittedManager.length == 1) splittedManager(0) else splittedManager(1)

      ErEndpoint(host = managerHost, port = managerPort.toInt)
    }
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    this.port = cmd.getOptionValue('p', "0").toInt

    val rollServer = NettyServerBuilder.forAddress(new InetSocketAddress(this.port))
      .maxInboundMetadataSize(1024*1024)
      .addService(new CommandService)
      .build
    rollServer.start()
    this.port = rollServer.getPort
    StaticErConf.setPort(port)
    logInfo(s"server started at ${port}")
    // job
    reportCM(sessionId, args, port)


    logInfo("heartbeated")
  }
}
