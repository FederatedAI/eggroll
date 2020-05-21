package com.webank.eggroll.rollframe

import java.io.File
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta.{ErEndpoint, ErPair, ErProcessor, ErTask}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.commons.cli.CommandLine

class EggFrameBootstrap extends BootstrapBase with Logging {

  private var cmd: CommandLine = _

  override def init(args: Array[String]): Unit = {
    cmd = CommandArgsUtils.parseArgs(args)

    val sessionId = cmd.getOptionValue("session-id")
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    val confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    if (StaticErConf.getBoolean("arrow.enable_unsafe_memory_access", defaultValue = false)) {
      System.setProperty("arrow.enable_unsafe_memory_access", "true")
      logInfo(s"EggFrame unSafe :${System.getProperty("arrow.enable_unsafe_memory_access")}")
    } else {
      logInfo(s"EggFrame unSafe :${System.getProperty("arrow.enable_unsafe_memory_access")}")
    }

    CommandRouter.register(
      serviceName = "EggFrame.runTask",
      serviceParamTypes = Array(classOf[ErTask]),
      serviceResultTypes = Array(classOf[ErPair]),
      routeToClass = classOf[EggFrame],
      routeToMethodName = "runTask")
  }

  def reportStatus(myCommandPort: Int, myTransferPort: Int): Unit = {
    // todo:2: heartbeat service
    val sessionId = cmd.getOptionValue("session-id")
    val clusterManager = cmd.getOptionValue("cluster-manager", "localhost:4670")
    //    val nodeManager = cmd.getOptionValue("node-manager", "localhost:9394")
    val serverNodeId = cmd.getOptionValue("server-node-id", "0").toLong
    val processorId = cmd.getOptionValue("processor-id", "0").toLong

    val clusterManagerClient = new ClusterManagerClient(ErEndpoint(clusterManager))

    val options = new ConcurrentHashMap[String, String]()
    options.put(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    // ignore heart beat when debugging
    if (processorId > 0) {
      val processName = ManagementFactory.getRuntimeMXBean.getName
      val pid = processName.split(StringConstants.AT, 2)(0).toInt
      val myself = ErProcessor(
        id = processorId,
        serverNodeId = serverNodeId,
        processorType = ProcessorTypes.EGG_FRAME,
        commandEndpoint = ErEndpoint("localhost", myCommandPort),
        transferEndpoint = ErEndpoint("localhost", myTransferPort),
        pid = pid,
        options = options,
        status = ProcessorStatus.RUNNING)
      logInfo("ready to heartbeat")
      clusterManagerClient.heartbeat(myself)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
          logInfo(s"""*** ${ProcessorTypes.EGG_FRAME} exit gracefully. sessionId: $sessionId, serverNodeId: $serverNodeId, processorId: $processorId, port: $myCommandPort ***""")
          val terminatedSelf = myself.copy(status = ProcessorStatus.STOPPED)
          clusterManagerClient.heartbeat(terminatedSelf)
          this.interrupt()
        }
      })
      logInfo(s"""heartbeated processorId: $processorId""")
    }
  }

  override def start(): Unit = {
    val specPort = cmd.getOptionValue("port", "0").toInt
    val specTransferPort = cmd.getOptionValue("transfer-port", "0").toInt
    val transferServer = new NioTransferEndpoint()
    transferServer.runServer("0.0.0.0", specTransferPort)
    val transferPort = transferServer.getPort
    logInfo(s"""server started at transferPort: $transferPort""")
    val cmdServer = NettyServerBuilder.forAddress(new InetSocketAddress(specPort))
      .maxInboundMetadataSize(1024 * 1024)
      .maxInboundMessageSize(1024 * 1024 * 1024)
      .addService(new CommandService)
      .build
    cmdServer.start()
    val port = cmdServer.getPort
    //    StaticErConf.setPort(port)
    logInfo(s"""server started at $port""")
    reportStatus(port, transferPort)
  }
}
