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

package com.webank.eggroll.rollpair.component

import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic.AtomicInteger

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.{ProcessorStatus, ProcessorTypes}
import com.webank.eggroll.core.datastructure.RollContext
import com.webank.eggroll.core.meta._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RollPairContext(sessionMeta: ErSessionMeta) extends RollContext {

  // todo: multiple servicer in one context
  private var servicer: ErProcessor = _
  private var processorBatches: ErProcessorBatch = _
  private var serverCluster: ErServerCluster = _
  private val nodeIdToProcessors = new ConcurrentSkipListMap[Long, ErProcessorBatch]()

  def getOrCreateServicer(nodeManagerEndpoint: ErEndpoint): ErProcessor = {
    val nodeManagerClient = new NodeManagerClient(nodeManagerEndpoint)
    val servicerBatch = nodeManagerClient.getOrCreateServicer(sessionMeta)

    servicer = servicerBatch.processors.head

    servicer
  }

  def getOrCreateProcessorBatch(serverCluster: ErServerCluster): ErProcessorBatch = {
    this.serverCluster = serverCluster
    val serverNodes = serverCluster.serverNodes
    val processorCount = new AtomicInteger(0)
    serverNodes.foreach(node => {
      val nodeManagerClient = new NodeManagerClient(node.endpoint)
      val processorBatch = nodeManagerClient.getOrCreateProcessorBatch(sessionMeta)
      nodeIdToProcessors.put(node.id, processorBatch)
      processorCount.addAndGet(processorBatch.processors.length)
    })

    val clusterProcessors = new Array[ErProcessor](processorCount.get())

    var i = 0
    nodeIdToProcessors.forEach((nodeId, processorBatch) => {
      processorBatch.processors.foreach(p => {
        clusterProcessors.update(i, p)
        i += 1
      })
    })

    ErProcessorBatch(processors = clusterProcessors, tag = ProcessorTypes.EGG_PAIR)
  }

  def registerServicer(processor: ErProcessor): ErProcessor = {
    if (servicer != null) {
      throw new IllegalStateException("RollPairServicer is not null")
    }

    if (!ProcessorStatus.RUNNING.equals(processor.status)) {
      throw new IllegalStateException(s"register: servicer is not running. current status: ${processor.status}")
    }

    servicer = processor
    processor
  }
}
