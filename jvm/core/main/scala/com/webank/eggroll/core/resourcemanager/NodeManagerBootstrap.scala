package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.containers.ContainersServiceHandler
import com.webank.eggroll.core.ex.grpc.NodeManagerExtendTransferService
import com.webank.eggroll.core.meta.{ErProcessor, ErResourceAllocation, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.NodeResourceManager.tryNodeHeartbeat
import com.webank.eggroll.core.session.{ExtendEnvConf, StaticErConf}
import com.webank.eggroll.core.transfer.GrpcServerUtils
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import io.grpc.Server

import java.io.File
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
    var extendConfPath:String =  this.confPath.replace("eggroll.properties","node-extend-env.properties")
    var extendEnvConfFile = new File(extendConfPath)
    if(extendEnvConfFile.exists()) {
        log.info("load extend env config file : "+extendConfPath)
        ExtendEnvConf.addProperties(extendConfPath)
    }
    // register services
    // To support parameters to NodeManagerService,
    // we instantiate a NodeManagerService instance here
    forkJoinPool = new ForkJoinPool()
    val executionContext = ExecutionContext.fromExecutorService(forkJoinPool)


    val nodeManagerJobService =  ContainersServiceHandler.getOrCreate(executionContext)

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

    CommandRouter.register_handler(
      ContainerCommands.startJobContainers.uriString,
      args => nodeManagerJobService.startJobContainers(args(0))
    )

    CommandRouter.register_handler(
      ContainerCommands.killJobContainers.uriString,
      args => nodeManagerJobService.killJobContainers(args(0))
    )

    CommandRouter.register_handler(
      ContainerCommands.stopJobContainers.uriString,
      args => nodeManagerJobService.stopJobContainers(args(0))
    )

    CommandRouter.register_handler(
      ContainerCommands.downloadContainers.uriString,
      args => nodeManagerJobService.downloadContainers(args(0))
    )

    CommandRouter.register(serviceName = ResouceCommands.resourceAllocation.uriString,
      serviceParamTypes = Array(classOf[ErResourceAllocation]),
      serviceResultTypes = Array(classOf[ErResourceAllocation]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = ResouceCommands.resourceAllocation.getName()
    )

    CommandRouter.register(serviceName = ResouceCommands.queryNodeResource.uriString,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = ResouceCommands.queryNodeResource.getName()
    )
    CommandRouter.register(serviceName = ResouceCommands.checkNodeProcess.uriString,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = ResouceCommands.checkNodeProcess.getName()
    )


    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    this.port = cmd.getOptionValue('p', StaticErConf.getProperty(
      NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, "9394")).toInt
    // StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    // TODO:0: get from cluster manager
    StaticErConf.addProperty(ResourceManagerConfKeys.SERVER_NODE_ID, "2")
  }

  override def start(): Unit = {
    if (this.port < 0) {
      this.port = 0
    }
    StaticErConf.addProperty(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, this.port.toString)

    server = GrpcServerUtils.createServer(
      port = this.port, grpcServices = List(new CommandService,new NodeManagerExtendTransferService))

    server.start()
    this.port = server.getPort

    NodeResourceManager.start();

    // TODO:0: why ?
    //    StaticErConf.setPort(this.port)
    val msg = s"server started at ${this.port}"
    println(msg)
    logInfo(msg)
  }

  override def shutdown(): Unit = {
    println("shutting down")
    NodeManagerMeta.status=ServerNodeStatus.LOSS
    tryNodeHeartbeat()
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
