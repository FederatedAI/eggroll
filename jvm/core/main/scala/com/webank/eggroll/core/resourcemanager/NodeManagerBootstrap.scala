package com.webank.eggroll.core.resourcemanager

import java.io.File
import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{CoreConfKeys, NodeManagerCommands, NodeManagerConfKeys, ResourceManagerConfKeys}
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta, ErJobMeta}
import com.webank.eggroll.core.resourcemanager.job.NodeManagerJobService
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcServerUtils
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import io.grpc.Server

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool

class NodeManagerBootstrap extends BootstrapBase with Logging {
  private var port = 0
  private var confPath = ""
  private var forkJoinPool: ForkJoinPool = _
  private var server: Server = _

  override def init(args: Array[String]): Unit = {
    val cmd = CommandArgsUtils.parseArgs(args = args)

    this.confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")

    // val sessionId = cmd.getOptionValue('s')
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    this.port = cmd.getOptionValue('p', StaticErConf.getProperty(
      NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, "9394")).toInt
    // StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    // TODO:0: get from cluster manager
    StaticErConf.addProperty(ResourceManagerConfKeys.SERVER_NODE_ID, "2")

    // register services
    // To support parameters to NodeManagerService,
    // we instantiate a NodeManagerService instance here
    forkJoinPool = new ForkJoinPool()
    val executionContext = ExecutionContext.fromExecutorService(forkJoinPool)
    val nodeManagerJobService = new NodeManagerJobService()(executionContext)

    CommandRouter.register(serviceName = NodeManagerCommands.startContainers.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = NodeManagerCommands.startContainers.getName())

    CommandRouter.register(serviceName = NodeManagerCommands.stopContainers.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = NodeManagerCommands.stopContainers.getName())

    CommandRouter.register(serviceName = NodeManagerCommands.killContainers.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = NodeManagerCommands.killContainers.getName())

    CommandRouter.register(serviceName = NodeManagerCommands.heartbeat.uriString,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = NodeManagerCommands.heartbeat.getName())

    CommandRouter.register(serviceName = NodeManagerCommands.startJobContainers.uriString,
      serviceParamTypes = Array(classOf[ErJobMeta]),
      serviceResultTypes = Array(classOf[ErJobMeta]),
      routeToClass = classOf[NodeManagerJobService],
      routeToMethodName = NodeManagerCommands.startJobContainers.getName(),
      routeToCallBasedClassInstance = nodeManagerJobService
    )
  }

  override def start(): Unit = {
    if (this.port < 0) {
      this.port = 0
    }

    server = GrpcServerUtils.createServer(
      port = this.port, grpcServices = List(new CommandService))

    server.start()
    this.port = server.getPort
    StaticErConf.addProperty(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, this.port.toString)

    // TODO:0: why ?
    //    StaticErConf.setPort(this.port)
    val msg = s"server started at ${this.port}"
    println(msg)
    logInfo(msg)
  }
  override def shutdown(): Unit = {
    println("shutting down")
    // Gracefully shut down the ForkJoinPool
    if (forkJoinPool != null) {
      forkJoinPool.shutdown()
      try {
        if (!forkJoinPool.awaitTermination(5, TimeUnit.SECONDS)) {
          logWarning("ForkJoinPool did not terminate in 30 seconds. Forcing shutdown.")
          forkJoinPool.shutdownNow()
        }
      } catch {
        case ex: InterruptedException =>
          logWarning("ForkJoinPool shutdown was interrupted. Forcing shutdown.")
          forkJoinPool.shutdownNow()
      }
    }
    if (server != null) {
      println("shutting down server")
      server.shutdown()
      println("server shutdown done")
    }
    println("shutting down done")
  }
}
