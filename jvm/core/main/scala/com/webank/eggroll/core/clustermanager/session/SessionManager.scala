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

package com.webank.eggroll.core.clustermanager.session

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.clustermanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.constant.{ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.meta.{ErProcessor, ErProcessorBatch, ErServerCluster, ErServerNode, ErSession, ErSessionMeta}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionManager {
  private val activeSessions = new ConcurrentHashMap[String, ErProcessorBatch]()

  def getOrCreateSession(sessionMeta: ErSessionMeta): ErProcessorBatch = {
    val sessionId = sessionMeta.id
    if (!activeSessions.containsKey(sessionId)) this.synchronized {
      if (!activeSessions.containsKey(sessionId)) {
        val healthyNodeExample = ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = ServerNodeTypes.NODE_MANAGER)
        val serverNodeCrudOperator = new ServerNodeCrudOperator()

        val healthyCluster = serverNodeCrudOperator.getServerNodes(healthyNodeExample)

        val deployer = new DefaultClusterDeployer(
          sessionMeta = sessionMeta,
          rollCluster = healthyCluster.copy(serverNodes = Array(healthyCluster.serverNodes.head)),
          eggCluster = healthyCluster)

        val rolls = deployer.createRolls()
        val eggs = deployer.createEggs()

        val processorBatch = ErProcessorBatch(processors = rolls.processors ++ eggs.processors, tag = sessionId)

        activeSessions.put(sessionId, processorBatch)
      }
    }

    activeSessions.get(sessionId)
  }

  def getSession(sessionId: String): ErProcessorBatch = this.synchronized {
    activeSessions.getOrDefault(sessionId, null)
  }

  def stopSession(session: ErSessionMeta): ErProcessorBatch = this.synchronized {
    val sessionId = session.id
    if (activeSessions.containsKey(sessionId)) {
      activeSessions.remove(sessionId)
    }

    null
  }
}

class DefaultClusterDeployer(sessionMeta: ErSessionMeta,
                             rollCluster: ErServerCluster,
                             eggCluster: ErServerCluster) {

  def createRolls(): ErProcessorBatch = {
    val rolls = ArrayBuffer[ErProcessor]()
    rolls.sizeHint(rollCluster.serverNodes.length)

    rollCluster.serverNodes.foreach(n => {
      val nodeManagerClient = new NodeManagerClient(n.endpoint)
      val processorBatch = nodeManagerClient.getOrCreateRolls(sessionMeta)
      val host = n.endpoint.host

      val i = new AtomicInteger(0)
      processorBatch.processors.foreach(p => {
        val curI = i.getAndIncrement()
        val populated = p.copy(id = curI, commandEndpoint = p.commandEndpoint.copy(host = host), dataEndpoint = p.dataEndpoint.copy(host = host), tag = s"${p.processorType}-${n.id}-${curI}")
        rolls += populated
      })
    })

    ErProcessorBatch(processors = rolls.toArray)
  }

  def createEggs(): ErProcessorBatch = {
    val eggs = ListBuffer[ErProcessor]()

    eggCluster.serverNodes.foreach(n => {
      val nodeManagerClient = new NodeManagerClient(n.endpoint)
      val processorBatch = nodeManagerClient.getOrCreateEggs(sessionMeta)

      val i = new AtomicInteger(0)
      val host = n.endpoint.host
      processorBatch.processors.foreach(p => {
        val curI = i.getAndIncrement()
        val populated = p.copy(id = curI, commandEndpoint = p.commandEndpoint.copy(host = host), dataEndpoint = p.dataEndpoint.copy(host = host), tag = s"${p.processorType}-${n.id}-${curI}")
        eggs += populated
      })
    })

    ErProcessorBatch(processors = eggs.toArray)
  }
}