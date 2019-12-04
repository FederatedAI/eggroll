package com.webank.eggroll.core

import com.webank.eggroll.core.client.{ClusterManagerClient, NodeManagerClient}
import com.webank.eggroll.core.constant.{DeployConfKeys, SessionStatus}
import com.webank.eggroll.core.meta.{ErProcessorBatch, ErSessionMeta}

import scala.collection.JavaConverters._
import scala.util.Random

trait ErDeploy

class ErStandaloneDeploy(sessionMeta: ErSessionMeta, options: Map[String, String] = Map()) extends ErDeploy {
  private val managerPort = options.getOrElse("eggroll.standalone.manager.port", "4670").toInt
  val cmClient: ClusterManagerClient = new ClusterManagerClient("localhost", managerPort)
  cmClient.registerSession(sessionMeta, ErProcessorBatch())
}
class ErSession(sessionId: String = s"er_session_${System.currentTimeMillis()}_${new Random().nextInt(9999)}",
                name:String="", tag:String="", options:Map[String, String] = Map()) {

  private val sessionMeta = ErSessionMeta(id = sessionId, name=name, status = SessionStatus.NEW,
    options=options.asJava, tag=tag)
  private val deployClient = if(options.getOrElse(DeployConfKeys.CONFKEY_DEPLOY_MODE, "standalone") == "standalone") {
    new ErStandaloneDeploy(sessionMeta)
  } else {
    throw new NotImplementedError("doing")
  }
}
