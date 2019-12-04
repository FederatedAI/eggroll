package com.webank.eggroll.core

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{DeployConfKeys, ProcessorStatus, ProcessorTypes, SessionStatus}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErProcessorBatch, ErSessionMeta}

import scala.collection.JavaConverters._
import scala.util.Random

trait ErDeploy

class ErStandaloneDeploy(sessionMeta: ErSessionMeta, options: Map[String, String] = Map()) extends ErDeploy {
  private val managerPort = options.getOrElse("eggroll.standalone.manager.port", "4670").toInt
  val rolls: List[ErProcessor] = List(ErProcessor(
    id=0, serverNodeId = 0, processorType = ProcessorTypes.ROLL_PAIR_SERVICER,
    status = ProcessorStatus.RUNNING, commandEndpoint = ErEndpoint("localhost", managerPort)))
  private val eggPorts = options.getOrElse("eggroll.standalone.egg.ports", "20001").split(",").map(_.toInt)
  val eggs: Map[Int, List[ErProcessor]] = Map(0 ->
    eggPorts.map(eggPort => ErProcessor(
      id=0, serverNodeId = 0, processorType = ProcessorTypes.EGG_PAIR,
      status = ProcessorStatus.RUNNING, commandEndpoint = ErEndpoint("localhost", eggPort), dataEndpoint = ErEndpoint("localhost", eggPort))
    ).toList
  )
  val cmClient: ClusterManagerClient = new ClusterManagerClient("localhost", managerPort)
  cmClient.registerSession(sessionMeta, ErProcessorBatch(processors = (rolls ++ eggs(0)).toArray))
}
class ErSession(val sessionId: String = s"er_session_${System.currentTimeMillis()}_${new Random().nextInt(9999)}",
                name:String="", tag:String="", options:Map[String, String] = Map()) {

  private val sessionMeta = ErSessionMeta(id = sessionId, name=name, status = SessionStatus.NEW,
    options=options.asJava, tag=tag)
  private val deployClient = if(options.getOrElse(DeployConfKeys.CONFKEY_DEPLOY_MODE, "standalone") == "standalone") {
    new ErStandaloneDeploy(sessionMeta)
  } else {
    throw new NotImplementedError("doing")
  }
  val cmClient: ClusterManagerClient = deployClient.cmClient
  val rolls: List[ErProcessor] = deployClient.rolls
  val eggs: Map[Int, List[ErProcessor]] = deployClient.eggs

}
