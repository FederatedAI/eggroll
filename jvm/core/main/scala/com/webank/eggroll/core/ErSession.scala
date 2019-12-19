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
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.TimeUtils

import scala.util.Random

trait ErDeploy

class ErStandaloneDeploy(sessionMeta: ErSessionMeta, options: Map[String, String] = Map()) extends ErDeploy {
  private val managerPort = options.getOrElse("eggroll.standalone.manager.port", "4670").toInt
  private val eggPorts = options.getOrElse("eggroll.standalone.egg.ports", "20001").split(",").map(_.toInt)
  private val eggTransferPorts = options.getOrElse("eggroll.standalone.egg.transfer.ports", "20002").split(",").map(_.toInt)
  private val selfServerNodeId = options.getOrElse("eggroll.standalone.server.node.id", "2").toLong

  val rolls: Array[ErProcessor] = Array(ErProcessor(
    id = 1,
    serverNodeId = selfServerNodeId,
    processorType = ProcessorTypes.ROLL_PAIR_MASTER,
    status = ProcessorStatus.RUNNING,
    commandEndpoint = ErEndpoint("localhost", managerPort)))

  val eggs: Map[Long, Array[ErProcessor]] = Map(selfServerNodeId ->
    eggPorts.zip(eggTransferPorts).map(ports => ErProcessor(
      id = 1,
      serverNodeId = selfServerNodeId,
      processorType = ProcessorTypes.EGG_PAIR,
      status = ProcessorStatus.RUNNING,
      commandEndpoint = ErEndpoint("localhost", ports._1),
      transferEndpoint = ErEndpoint("localhost", ports._2))))

  val processors = rolls ++ eggs(selfServerNodeId)

  val cmClient: ClusterManagerClient = new ClusterManagerClient("localhost", managerPort)
  val existingSession = cmClient.getSession(sessionMeta)
  if (existingSession.processors.length == 0) cmClient.registerSession(sessionMeta.copy(processors = processors))
}

class ErSession(val sessionId: String = s"er_session_${TimeUtils.getNowMs()}_${new Random().nextInt(9999)}",
                name: String = "", tag: String = "", options: Map[String, String] = Map()) {

  var status = SessionStatus.NEW
  private val sessionMeta = ErSessionMeta(
    id = sessionId,
    name=name,
    status = status,
    options=options,
    tag=tag)
  private val deployClient = if(options.getOrElse(DeployConfKeys.CONFKEY_DEPLOY_MODE, "standalone") == "standalone") {
    new ErStandaloneDeploy(sessionMeta)
  } else {
    throw new NotImplementedError("doing")
  }

  val cmClient: ClusterManagerClient = deployClient.cmClient
  private val rolls = deployClient.rolls
  private val eggs = deployClient.eggs
  private val processors = deployClient.processors

  def routeToEgg(partition: ErPartition): ErProcessor = {
    val serverNodeId = partition.processor.serverNodeId
    val eggCountOnServerNode = eggs(serverNodeId).length
    val eggIdx = partition.id / eggCountOnServerNode % eggCountOnServerNode

    eggs(serverNodeId)(eggIdx)
  }
}
