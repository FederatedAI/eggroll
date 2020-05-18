package com.webank.eggroll.core.resourcemanager

import java.io.File

import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{CoreConfKeys, NodeManagerCommands, NodeManagerConfKeys, ResourceManagerConfKeys}
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcServerUtils
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}

class NodeManagerBootstrap extends BootstrapBase with Logging {
  private var port = 0
  private var confPath = ""
  override def init(args: Array[String]): Unit = {
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

    val cmd = CommandArgsUtils.parseArgs(args = args)

    this.confPath = cmd.getOptionValue('c', "./jvm/core/main/resources/cluster-manager.properties")

    // val sessionId = cmd.getOptionValue('s')
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    this.port = cmd.getOptionValue('p', StaticErConf.getProperty(
      NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT,"9394")).toInt
    // StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    // TODO:0: get from cluster manager
    StaticErConf.addProperty(ResourceManagerConfKeys.SERVER_NODE_ID, "2")
  }

  override def start(): Unit = {
    var port = StaticErConf.getPort()
    if (port < 0) {
      port = 0
    }

    StaticErConf.addProperty(NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT, port.toString)
    val server = GrpcServerUtils.createServer(
      port = this.port, grpcServices = List(new CommandService))

    server.start()
    this.port = server.getPort

    // TODO:0: why ?
//    StaticErConf.setPort(this.port)
    val msg = s"server started at ${port}"
    println(msg)
    logInfo(msg)
  }
}
