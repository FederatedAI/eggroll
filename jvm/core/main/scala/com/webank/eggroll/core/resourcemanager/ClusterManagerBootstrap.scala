package com.webank.eggroll.core.resourcemanager

import java.io.File

import org.apache.commons.lang3.StringUtils
import com.webank.eggroll.core.BootstrapBase
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, CoreConfKeys, MetadataCommands, SessionCommands}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.metadata.{ServerNodeCrudOperator, StoreCrudOperator}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.{GrpcClientUtils, GrpcServerUtils}
import com.webank.eggroll.core.util.{CommandArgsUtils, Logging}

class ClusterManagerBootstrap extends BootstrapBase with Logging {
  private var port = 0
  private var standaloneTag = "0"
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

    CommandRouter.register(serviceName = MetadataCommands.getStoreFromNamespaceServiceName,
      serviceParamTypes = Array(classOf[ErStore]),
      serviceResultTypes = Array(classOf[ErStoreList]),
      routeToClass = classOf[StoreCrudOperator],
      routeToMethodName = MetadataCommands.getStoreFromNamespace)

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

    CommandRouter.register(serviceName = SessionCommands.killSession.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[SessionManagerService],
      routeToMethodName = SessionCommands.killSession.getName())

    CommandRouter.register(serviceName = SessionCommands.killAllSessions.uriString,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErSessionMeta]),
      routeToClass = classOf[SessionManagerService],
      routeToMethodName = SessionCommands.killAllSessions.getName())

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

    //this.sessionId = cmd.getOptionValue('s')
    val confPath = cmd.getOptionValue('c', "./conf/eggroll.properties")
    standaloneTag = System.getProperty("eggroll.standalone.tag", "")

    StaticErConf.addProperties(confPath)
    val confFile = new File(confPath)
    StaticErConf.addProperty(CoreConfKeys.STATIC_CONF_PATH, confFile.getAbsolutePath)
    logInfo(s"conf file: ${confFile.getAbsolutePath}")
    this.port = cmd.getOptionValue('p', cmd.getOptionValue('p', StaticErConf.getProperty(
      ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT,"4670"))).toInt

    if(StringUtils.isBlank(standaloneTag)) {
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        logWarning("****** Shutting down Cluster Manager ******")
        logInfo("Shutting down cluster manager. Force terminating all grpc channel")
        GrpcClientUtils.shutdownNow()
        logInfo("All grpc client channels are shut down")
        logInfo("Shutting down cluster manager. Force terminating NEW / ACTIVE sessions")
        val sessionManagerService = new SessionManagerService()
        sessionManagerService.killAllSessions(ErSessionMeta())

        logWarning("****** All sessions stopped / killed. Cluster Manager exiting gracefully ******")
      }))
    }
    //StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
  }

  override def start(): Unit = {
    // TODO:0: use user's config
    val server = GrpcServerUtils.createServer(port = this.port, grpcServices = List(new CommandService))
    server.start()
    this.port = server.getPort

    StaticErConf.setPort(port)
    logInfo(s"${standaloneTag} server started at port ${port}")
    println(s"${standaloneTag} server started at port ${port}")
  }
}
