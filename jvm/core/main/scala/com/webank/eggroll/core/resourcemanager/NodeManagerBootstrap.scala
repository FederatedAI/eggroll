package com.webank.eggroll.core.resourcemanager

import java.io.File
import java.net.InetSocketAddress

import com.webank.eggroll.core.Bootstrap
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{CoreConfKeys, NodeManagerCommands, ResourceManagerConfKeys}
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

class NodeManagerBootstrap extends Bootstrap with Logging {
  private var port = 0
  private var confPath = ""
  override def init(args: Array[String]): Unit = {
/*
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
    */

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

    CommandRouter.register(serviceName = NodeManagerCommands.heartbeat.uriString,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[NodeManagerService],
      routeToMethodName = NodeManagerCommands.heartbeat.getName())

    val cmd = CommandArgsUtils.parseArgs(args = args)
    this.port = cmd.getOptionValue('p', "9394").toInt
    this.confPath = cmd.getOptionValue('c', "./jvm/core/main/resources/cluster-manager.properties.local")
    // val sessionId = cmd.getOptionValue('s')
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    // StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    // TODO:0: get from cluster manager or database
    StaticErConf.addProperty(ResourceManagerConfKeys.SERVER_NODE_ID, "2")
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
