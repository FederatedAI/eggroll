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

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.clustermanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.constant._
import com.webank.eggroll.core.meta._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SessionManager {
  private val activeSessions = new ConcurrentHashMap[String, ErSessionDeployment]()

  // (sessionId, (bindingId, ErProcessorBatch))
  private val activeSessionBound = new ConcurrentHashMap[String, ConcurrentHashMap[String, ErProcessorBatch]]()

  activeSessions.put(StringConstants.UNKNOWN,
    ErSessionDeployment(
      id = StringConstants.UNKNOWN,
      serverCluster = ErServerCluster(),
      rolls = Array.empty,
      eggs = Map.empty))

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

        val newDeployment = ErSessionDeployment(
          id = sessionId,
          serverCluster = healthyCluster,
          rolls = rolls.processors,
          eggs = eggs)
        activeSessions.put(sessionId, newDeployment)
      }
    }
    val deployment = activeSessions.get(sessionId)

    deployment.toErProcessorBatch()
  }

  def register(sessionMeta: ErSessionMeta, processorBatch: ErProcessorBatch): ErProcessorBatch = {
    val sessionId = sessionMeta.id
    // todo: register or get
/*
    if (activeSessions.containsKey(sessionId)) {
      throw new IllegalStateException("session already exists")
    }
*/

    val rolls = ArrayBuffer[ErProcessor]()
    val eggs = mutable.Map[Long, ArrayBuffer[ErProcessor]]()

    val hosts = new util.ArrayList[String](processorBatch.processors.length)
    processorBatch.processors.foreach(p => hosts.add(p.transferEndpoint.host))

    val serverNodeCrudOperator = new ServerNodeCrudOperator
    val serverCluster = serverNodeCrudOperator.getServerClusterByHosts(hosts)
    val hostToNodeId = mutable.Map[String, Long]()

    serverCluster.serverNodes.foreach(n => {
      hostToNodeId(n.endpoint.host) = n.id
    })

    // todo:1: eliminate with MetaModel class definition
    processorBatch.processors.foreach(p => {
      val pWithServerNodeInfo = p.copy(serverNodeId = hostToNodeId(p.commandEndpoint.host))
      if (ProcessorTypes.EGG_PAIR.equals(p.processorType)) {
        eggs.get(pWithServerNodeInfo.serverNodeId) match {
          case Some(b) => b += pWithServerNodeInfo
          case None =>
            val arrayBuffer = ArrayBuffer[ErProcessor]()
            arrayBuffer += pWithServerNodeInfo
            eggs += (pWithServerNodeInfo.serverNodeId -> arrayBuffer)
        }
      } else if (ProcessorTypes.ROLL_PAIR_MASTER.equals(p.processorType)) {
        rolls += p.copy(serverNodeId = hostToNodeId(p.commandEndpoint.host))
      }
    })

    // todo: find and populate serverCluster
    val newDeployment = ErSessionDeployment(
      id = sessionId,
      serverCluster = serverCluster,
      rolls = rolls.toArray,
      eggs = eggs.mapValues(_.toArray).toMap)

    activeSessions.put(sessionId, newDeployment)

    activeSessions.get(sessionId).toErProcessorBatch()
  }

  def getSession(sessionId: String): ErProcessorBatch = {
    val result = activeSessions.getOrDefault(sessionId, null)

    if (result != null) result.toErProcessorBatch()
    else null
  }

  def addSession(sessionId: String, deployment: ErSessionDeployment): Unit = {
    if (activeSessions.contains(sessionId)) {
      throw new IllegalArgumentException(s"sessionId ${sessionId} already exists")
    }

    activeSessions.put(sessionId, deployment)
  }

  def stopSession(session: ErSessionMeta): ErProcessorBatch = this.synchronized {
    val sessionId = session.id
    if (activeSessions.containsKey(sessionId)) {
      activeSessions.remove(sessionId)
    }

    null
  }

  def getSessionDeployment(sessionId: String): ErSessionDeployment = {
    activeSessions.get(sessionId)
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
        val populated = p.copy(
          id = curI,
          commandEndpoint = p.commandEndpoint.copy(host = host),
          transferEndpoint = p.transferEndpoint.copy(host = host),
          tag = s"${p.processorType}-${n.id}-${curI}")
        rolls += populated
      })
    })

    ErProcessorBatch(processors = rolls.toArray)
  }

  def createEggs(): Map[Long, Array[ErProcessor]] = {
    val eggs = mutable.Map[Long, Array[ErProcessor]]()

    eggCluster.serverNodes.foreach(n => {
      val nodeManagerClient = new NodeManagerClient(n.endpoint)
      val processorBatch = nodeManagerClient.getOrCreateEggs(sessionMeta)

      val i = new AtomicInteger(0)
      val host = n.endpoint.host

      val populatedEggs = new Array[ErProcessor](processorBatch.processors.length)
      processorBatch.processors.foreach(p => {
        val curI = i.getAndIncrement()
        val populated = p.copy(id = curI, serverNodeId = n.id, commandEndpoint = p.commandEndpoint.copy(host = host), transferEndpoint = p.transferEndpoint.copy(host = host), tag = s"${p.processorType}-${n.id}-${curI}")

        populatedEggs.update(curI, populated)
      })

      eggs += (n.id -> populatedEggs)
    })

    eggs.toMap
  }
}