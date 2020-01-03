package com.webank.eggroll.core.resourcemanager

import java.io.File
import java.net.InetSocketAddress

import com.webank.eggroll.core.Bootstrap
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{CoreConfKeys, MetadataCommands, SessionCommands}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.metadata.{ServerNodeCrudOperator, StoreCrudOperator}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

class ClusterManagerBootstrap extends Bootstrap with Logging {
  private var port = 0
  //private var sessionId = "er_session_null"
  override def init(args: Array[String]): Unit = {

    CommandRouter.register(serviceName = MetadataCommands.getServerNodeServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.getServerNode)

    CommandRouter.register(serviceName = MetadataCommands.getServerNodesServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerCluster]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.getServerNodes)

    CommandRouter.register(serviceName = MetadataCommands.getOrCreateServerNodeServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.getOrCreateServerNode)

    CommandRouter.register(serviceName = MetadataCommands.createOrUpdateServerNodeServiceName,
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[ServerNodeCrudOperator],
      routeToMethodName = MetadataCommands.createOrUpdateServerNode)

    CommandRouter.register(serviceName = MetadataCommands.getStoreServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStore]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.getStore)

    CommandRouter.register(serviceName = MetadataCommands.getOrCreateStoreServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStore]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.getOrCreateStore)

    CommandRouter.register(serviceName = MetadataCommands.deleteStoreServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStore]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.deleteStore)

    CommandRouter.register(serviceName = SessionCommands.getSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[SessionManagerService],
      routeToMethodName = SessionCommands.getSession.getName())

    CommandRouter.register(serviceName = SessionCommands.getOrCreateSession.uriString,
        serviceParamTypes = Array(classOf[ErSessionMeta]),
        serviceResultTypes = Array(classOf[ErSessionMeta]),
        routeToClass = classOf[SessionManagerService],
        routeToMethodName = SessionCommands.getOrCreateSession.getName())

    CommandRouter.register(serviceName = SessionCommands.stopSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[SessionManagerService],
      routeToMethodName = SessionCommands.stopSession.getName())

    CommandRouter.register(serviceName = SessionCommands.registerSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[SessionManagerService],
      routeToMethodName = SessionCommands.registerSession.getName())

    CommandRouter.register(serviceName = SessionCommands.heartbeat.uriString,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[SessionManagerService],
      routeToMethodName = SessionCommands.heartbeat.getName())

    val cmd = CommandArgsUtils.parseArgs(args = args)
    this.port = cmd.getOptionValue('p', "4670").toInt
    //this.sessionId = cmd.getOptionValue('s')
    val confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    //StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
  }

  override def start(): Unit = {
    // TODO:0: use user's config
    val clusterManager = NettyServerBuilder
      .forAddress(new InetSocketAddress(this.port))
      .addService(new CommandService)
      .maxInboundMessageSize(1024 * 1024 *1024)
      .maxInboundMetadataSize(1024 * 1024)
      .build()

    val server: Server = clusterManager.start()

    val port = server.getPort
    StaticErConf.setPort(port)
    logInfo(s"server started at port ${port}")
    println(s"server started at port ${port}")
  }
}
