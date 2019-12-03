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
import com.webank.eggroll.core.constant.{BindingStrategies, _}
import com.webank.eggroll.core.meta._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SessionManager {
  private val activeSessions = new ConcurrentHashMap[String, ErServerSessionDeployment]()

  activeSessions.put(StringConstants.UNKNOWN,
    new ErServerSessionDeployment(
      id = StringConstants.UNKNOWN,
      serverCluster = ErServerCluster(),
      rolls = Array.empty,
      eggs = Map.empty,
      partitionBindings = mutable.Map[String, ErPartitionBinding]()))

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

        val newDeployment = ErServerSessionDeployment(
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
    if (activeSessions.containsKey(sessionId)) {
      throw new IllegalStateException("session already exists")
    }

    val rolls = ArrayBuffer[ErProcessor]()
    val eggs = mutable.Map[Long, ArrayBuffer[ErProcessor]]()

    processorBatch.processors.foreach(p => {
      if (ProcessorTypes.EGG_PAIR.equals(p.processorType)) {
        eggs.get(p.serverNodeId) match {
          case Some(b) => b += p
          case None =>
            val arrayBuffer = ArrayBuffer[ErProcessor]()
            arrayBuffer += p
            eggs += (p.serverNodeId -> arrayBuffer)
        }
      } else if (ProcessorTypes.ROLL_PAIR_SERVICER.equals(p.processorType)) {
        rolls += p
      }
    })

    // todo: find and populate serverCluster
    val newDeployment = ErServerSessionDeployment(
      id = sessionId,
      serverCluster = ErServerCluster(),
      rolls = rolls.toArray,
      eggs = eggs.mapValues(_.toArray).toMap)

    activeSessions.put(sessionId, newDeployment)

    activeSessions.get(sessionId).toErProcessorBatch()
  }


  def getSession(sessionId: String): ErProcessorBatch = {
    activeSessions.getOrDefault(sessionId, null).toErProcessorBatch()
  }

  def stopSession(session: ErSessionMeta): ErProcessorBatch = this.synchronized {
    val sessionId = session.id
    if (activeSessions.containsKey(sessionId)) {
      activeSessions.remove(sessionId)
    }

    null
  }

  def hasBinding(sessionId: String, bindingId: String): Boolean = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    deployment != null && deployment.partitionBindings.contains(bindingId)
  }

  def addBinding(sessionId: String, binding: ErPartitionBinding): Unit = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    if (deployment == null) {
      throw new IllegalStateException(s"session ${sessionId} has not been deployed yet")
    }

    val bindings = deployment.partitionBindings
    if (bindings.contains(binding.id)) {
      throw new IllegalArgumentException(s"binding ${binding.id} in session ${sessionId} already exists")
    }

    bindings += (binding.id -> binding)
  }

  def getBinding(sessionId: String,
                 totalPartitions: Int,
                 serverNodeIds: Array[Long],
                 strategy: String = BindingStrategies.ROUND_ROBIN): ErPartitionBinding = {
    val bindingId = ErPartitionBinding.genId(
      sessionId = sessionId,
      totalPartitions = totalPartitions,
      serverNodeIds = serverNodeIds,
      strategy = strategy)

    getBinding(sessionId, bindingId)
  }

  def getBinding(sessionId: String, bindingId: String): ErPartitionBinding = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    if (deployment == null) {
      throw new IllegalStateException(s"session ${sessionId} has not been deployed yet")
    }

    deployment.partitionBindings.getOrElse(bindingId, null)
  }

  def createBinding(sessionId: String,
                    totalPartitions: Int,
                    serverNodeIds: Array[Long],
                    strategy: String = BindingStrategies.ROUND_ROBIN,
                    detailBindings: Array[ErProcessor] = Array.empty): ErPartitionBinding = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    if (deployment == null) {
      throw new IllegalStateException(s"session ${sessionId} has not been deployed yet")
    }

    val partitionToServerNodes = ArrayBuffer[Long]()
    partitionToServerNodes.sizeHint(totalPartitions)

    val nodesLength = serverNodeIds.length

    (0 until totalPartitions).foreach(partitionId => {
      partitionToServerNodes += serverNodeIds(partitionId % nodesLength)
    })

    val concatted = serverNodeIds.mkString(StringConstants.COMMA)
    val id = ErPartitionBinding.genId(sessionId, totalPartitions, serverNodeIds, strategy)
    var result = new ErPartitionBinding(id = id,
      partitionToServerNodes = partitionToServerNodes.toArray,
      totalPartitions = totalPartitions,
      bindingStrategy = strategy,
      detailBindings = detailBindings)

    if (deployment.partitionBindings.contains(id)) {
      result = deployment.partitionBindings.getOrElse(id, result)
    } else {
      deployment.partitionBindings += (id -> result)
    }

    result
  }

  def getSessionDeployment(sessionId: String): ErServerSessionDeployment = {
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
          dataEndpoint = p.dataEndpoint.copy(host = host),
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
        val populated = p.copy(id = curI, serverNodeId = n.id, commandEndpoint = p.commandEndpoint.copy(host = host), dataEndpoint = p.dataEndpoint.copy(host = host), tag = s"${p.processorType}-${n.id}-${curI}")

        populatedEggs.update(curI, populated)
      })

      eggs += (n.id -> populatedEggs)
    })

    eggs.toMap
  }
}