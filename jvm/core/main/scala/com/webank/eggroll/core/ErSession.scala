/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{DeployConfKeys, ProcessorStatus, ProcessorTypes, SessionStatus}
import com.webank.eggroll.core.meta.{ErEndpoint, ErPartition, ErProcessor, ErProcessorBatch, ErSessionDeployment, ErSessionMeta}
import com.webank.eggroll.core.util.TimeUtils

import scala.collection.JavaConverters._
import scala.util.Random

trait ErDeploy

class ErStandaloneDeploy(sessionMeta: ErSessionMeta, options: Map[String, String] = Map()) extends ErDeploy {
  private val managerPort = options.getOrElse("eggroll.standalone.manager.port", "4670").toInt
  val rolls: List[ErProcessor] = List(ErProcessor(
    id=0, serverNodeId = 0, processorType = ProcessorTypes.ROLL_PAIR_SERVICER,
    status = ProcessorStatus.RUNNING, commandEndpoint = ErEndpoint("localhost", managerPort)))
  private val eggPorts = options.getOrElse("eggroll.standalone.egg.ports", "20001").split(",").map(_.toInt)
  private val eggTransferPorts = options.getOrElse("eggroll.standalone.egg.transfer.ports", "20002").split(",").map(_.toInt)
  val eggs: Map[Int, List[ErProcessor]] = Map(0 ->
    eggPorts.zip(eggTransferPorts).map(ports => ErProcessor(
      id=0, serverNodeId = 0, processorType = ProcessorTypes.EGG_PAIR,
      status = ProcessorStatus.RUNNING, commandEndpoint = ErEndpoint("localhost", ports._1), transferEndpoint = ErEndpoint("localhost", ports._2))
    ).toList
  )
  val cmClient: ClusterManagerClient = new ClusterManagerClient("localhost", managerPort)
  cmClient.registerSession(sessionMeta, ErProcessorBatch(processors = (rolls ++ eggs(0)).toArray))
}
class ErSession(val sessionId: String = s"er_session_${TimeUtils.getNowMs()}_${new Random().nextInt(9999)}",
                name: String = "", tag: String = "", options: Map[String, String] = Map()) {

  var status = SessionStatus.NEW
  private val sessionMeta = ErSessionMeta(id = sessionId, name=name, status = status,
    options=options, tag=tag)
  private val deployClient = if(options.getOrElse(DeployConfKeys.CONFKEY_DEPLOY_MODE, "standalone") == "standalone") {
    new ErStandaloneDeploy(sessionMeta)
  } else {
    throw new NotImplementedError("doing")
  }

  val cmClient: ClusterManagerClient = deployClient.cmClient
  private val serverNodes = cmClient.getSessionServerNodes(sessionMeta = sessionMeta)
  private val rolls = cmClient.getSessionRolls(sessionMeta = sessionMeta)
  private val eggs = cmClient.getSessionEggs(sessionMeta = sessionMeta)

  val serverSessionDeployment = ErSessionDeployment(
    id = sessionId,
    serverCluster = serverNodes,
    rollProcessorBatch = rolls,
    eggProcessorBatch = eggs)

  def routeToEgg(partition: ErPartition): ErProcessor = {
    val serverNodeId = partition.processor.serverNodeId
    val eggCountOnServerNode = serverSessionDeployment.eggs(serverNodeId).length
    val eggIdx = partition.id / eggCountOnServerNode % eggCountOnServerNode

    serverSessionDeployment.eggs(serverNodeId)(eggIdx)
  }
}
