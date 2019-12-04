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
import com.webank.eggroll.core.clustermanager.dao.generated.model.ServerNodeExample
import com.webank.eggroll.core.clustermanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.constant.{BindingStrategies, _}
import com.webank.eggroll.core.meta._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SessionManager {
  private val activeSessions = new ConcurrentHashMap[String, ErServerSessionDeployment]()

  // (sessionId, (bindingId, ErProcessorBatch))
  private val activeSessionBound = new ConcurrentHashMap[String, ConcurrentHashMap[String, ErProcessorBatch]]()

  activeSessions.put(StringConstants.UNKNOWN,
    ErServerSessionDeployment(
      id = StringConstants.UNKNOWN,
      serverCluster = ErServerCluster(),
      rolls = Array.empty,
      eggs = Map.empty,
      partitionBindingPlans = mutable.Map[String, ErPartitionBindingPlan]()))

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

    val hosts = new util.ArrayList[String](processorBatch.processors.length)
    processorBatch.processors.foreach(p => hosts.add(p.dataEndpoint.host))

    val serverNodeCrudOperator = new ServerNodeCrudOperator
    val serverCluster = serverNodeCrudOperator.getServerClusterByHosts(hosts)
    val hostToNodeId = mutable.Map[String, Long]()

    serverCluster.serverNodes.foreach(n => {
      hostToNodeId(n.endpoint.host) = n.id
    })

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
      } else if (ProcessorTypes.ROLL_PAIR_SERVICER.equals(p.processorType)) {
        rolls += p.copy(serverNodeId = hostToNodeId(p.commandEndpoint.host))
      }
    })

    // todo: find and populate serverCluster
    val newDeployment = ErServerSessionDeployment(
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

  def addSession(sessionId: String, deployment: ErServerSessionDeployment): Unit = {
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

  def hasBinding(sessionId: String, bindingId: String): Boolean = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    deployment != null && deployment.partitionBindingPlans.contains(bindingId)
  }

  def addBinding(sessionId: String, binding: ErPartitionBindingPlan): Unit = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    if (deployment == null) {
      throw new IllegalStateException(s"session ${sessionId} has not been deployed yet")
    }

    val bindings = deployment.partitionBindingPlans
    if (bindings.contains(binding.id)) {
      throw new IllegalArgumentException(s"binding ${binding.id} in session ${sessionId} already exists")
    }

    bindings += (binding.id -> binding)
  }

  def getBindingPlan(sessionId: String,
                     totalPartitions: Int,
                     serverNodeIds: Array[Long],
                     strategy: String = BindingStrategies.ROUND_ROBIN): ErPartitionBindingPlan = {
    val bindingId = ErPartitionBindingPlan.genId(
      sessionId = sessionId,
      totalPartitions = totalPartitions,
      serverNodeIds = serverNodeIds,
      strategy = strategy)

    getBindingPlan(sessionId, bindingId)
  }

  def getBindingPlan(sessionId: String, bindingId: String): ErPartitionBindingPlan = {
    val deployment = activeSessions.getOrDefault(sessionId, null)
    if (deployment == null) {
      throw new IllegalStateException(s"session ${sessionId} has not been deployed yet")
    }

    deployment.partitionBindingPlans.getOrElse(bindingId, null)
  }

  def createBindingPlan(sessionId: String,
                        totalPartitions: Int,
                        serverNodeIds: Array[Long],
                        strategy: String = BindingStrategies.ROUND_ROBIN,
                        detailBindings: Array[ErProcessor] = Array.empty): ErPartitionBindingPlan = {
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
    val id = ErPartitionBindingPlan.genId(sessionId, totalPartitions, serverNodeIds, strategy)
    var result = new ErPartitionBindingPlan(id = id,
      partitionToServerNodes = partitionToServerNodes.toArray,
      totalPartitions = totalPartitions,
      bindingStrategy = strategy,
      detailBindings = detailBindings)

    if (deployment.partitionBindingPlans.contains(id)) {
      result = deployment.partitionBindingPlans.getOrElse(id, result)
    } else {
      deployment.partitionBindingPlans += (id -> result)
    }

    result
  }

  def getBoundProcessorBatch(sessionId: String, bindingPlanId: String): ErProcessorBatch = {
    if (!activeSessionBound.containsKey(sessionId)) {
      activeSessionBound.put(sessionId, new ConcurrentHashMap[String, ErProcessorBatch]())
    }
    val bounds = activeSessionBound.get(sessionId)

    var result = bounds.get(bindingPlanId)
    if (result != null) {
      return result
    }

    val deployment = getSessionDeployment(sessionId)
    if (deployment == null) {
      throw new IllegalStateException(s"session ${sessionId} has not been deployed yet")
    }

    result = deployment.getBoundErProcessorBatch(bindingPlanId = bindingPlanId)
    bounds.put(bindingPlanId, result)

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