package com.webank.eggroll.core.resourcemanager

import java.net.InetSocketAddress

import com.webank.eggroll.core.Bootstrap
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.NodeManagerCommands
import com.webank.eggroll.core.meta.{ErProcessor, ErProcessorBatch, ErSessionMeta}
import com.webank.eggroll.core.nodemanager.NodeManager
import com.webank.eggroll.core.nodemanager.NodeManager.{logInfo, registerRouter}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{Logging, MiscellaneousUtils}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

class NodeManagerBootstrap extends Bootstrap with Logging {
  private var port = 0
  override def init(args: Array[String]): Unit = {
    CommandRouter.register(serviceName = NodeManagerCommands.getOrCreateEggsServiceName,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[com.webank.eggroll.core.nodemanager.NodeManager],
      routeToMethodName = NodeManagerCommands.getOrCreateEggs)

    CommandRouter.register(serviceName = NodeManagerCommands.getOrCreateRollsServiceName,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[com.webank.eggroll.core.nodemanager.NodeManager],
      routeToMethodName = NodeManagerCommands.getOrCreateRolls)

    CommandRouter.register(serviceName = NodeManagerCommands.heartbeatServiceName,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[com.webank.eggroll.core.nodemanager.NodeManager],
      routeToMethodName = NodeManagerCommands.heartbeat)

    val cmd = MiscellaneousUtils.parseArgs(args = args)
    this.port = cmd.getOptionValue('p', "9394").toInt
  }

  override def start(): Unit = {
    val server = NettyServerBuilder
      .forAddress(new InetSocketAddress(this.port))
      .addService(new CommandService).build
    server.start()
    val port = server.getPort
    // TODO:0: why ?
//    StaticErConf.setPort(this.port)
    val msg = s"server started at ${port}"
    println(msg)
    logInfo(msg)
  }
}
