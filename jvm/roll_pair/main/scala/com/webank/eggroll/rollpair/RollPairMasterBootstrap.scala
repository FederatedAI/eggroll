package com.webank.eggroll.rollpair

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import _root_.io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import com.webank.eggroll.core.Bootstrap
import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{ProcessorStatus, ProcessorTypes, SessionConfKeys, StringConstants}
import com.webank.eggroll.core.meta.{ErEndpoint, ErJob, ErProcessor}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{Logging, MiscellaneousUtils}
import com.webank.eggroll.rollpair.component.RollPairMaster
import org.apache.commons.lang3.StringUtils

class RollPairMasterBootstrap extends Bootstrap with Logging {
  private var port = 0
  private var sessionId = "er_session_null"
  private var nodeManager = ""
  override def init(args: Array[String]): Unit = {
    CommandRouter.register(serviceName = RollPairMaster.rollMapValuesCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.mapValues)

    CommandRouter.register(serviceName = RollPairMaster.rollMapCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.runJob)

    CommandRouter.register(serviceName = RollPairMaster.rollMapPartitionsCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.mapPartitions)

    CommandRouter.register(serviceName = RollPairMaster.rollCollapsePartitionsCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.collapsePartitions)

    CommandRouter.register(serviceName = RollPairMaster.rollFlatMapCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.flatMap)

    CommandRouter.register(serviceName = RollPairMaster.rollGlomCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.glom)

    CommandRouter.register(serviceName = RollPairMaster.rollSampleCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.sample)

    CommandRouter.register(serviceName = RollPairMaster.rollFilterCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.filter)

    CommandRouter.register(serviceName = RollPairMaster.rollSubtractByKeyCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.subtractByKey)

    CommandRouter.register(serviceName = RollPairMaster.rollUnionCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.union)

    CommandRouter.register(serviceName = RollPairMaster.rollReduceCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.reduce)

    CommandRouter.register(serviceName = RollPairMaster.rollJoinCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.runJob)

    CommandRouter.register(serviceName = RollPairMaster.rollRunJobCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.runJob)

    CommandRouter.register(serviceName = RollPairMaster.rollPutAllCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.runJob)

    CommandRouter.register(serviceName = RollPairMaster.rollGetAllCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairMaster],
      routeToMethodName = RollPairMaster.runJob)

    val cmd = MiscellaneousUtils.parseArgs(args = args)
    this.port = cmd.getOptionValue('p', "0").toInt
    this.sessionId = cmd.getOptionValue('s', "UNKNOWN")
    this.nodeManager = cmd.getOptionValue("nm")
  }
  def reportCM(sessionId:String, nm:ErEndpoint, selfPort:Int):Unit = {
    // todo: get port from command line
    // todo: heartbeat service
    val nodeManagerClient = new NodeManagerClient(nm)
    val options = new ConcurrentHashMap[String, String]()
    options.put(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    val myself = ErProcessor(
      processorType = ProcessorTypes.ROLL_PAIR_MASTER,
      commandEndpoint = ErEndpoint("localhost", selfPort),
      transferEndpoint = ErEndpoint("localhost", selfPort),
      options = options,
      status = ProcessorStatus.RUNNING)

    logInfo("ready to heartbeat")
    nodeManagerClient.heartbeat(myself)

  }
  override def start(): Unit = {
    println(nodeManager)
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

    val rollServer = NettyServerBuilder.forAddress(new InetSocketAddress(this.port))
      .addService(new CommandService)
      .build
    rollServer.start()
    val port = rollServer.getPort
    StaticErConf.setPort(port)
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    logInfo(s"server started at ${port}")
    // job

    logInfo("server started at port 20000")

    reportCM(sessionId, managerEndpoint, port)


    logInfo("heartbeated")
  }
}
