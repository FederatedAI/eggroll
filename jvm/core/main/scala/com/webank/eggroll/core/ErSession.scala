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
import com.webank.eggroll.core.constant.{ProcessorTypes, SessionStatus}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.{RuntimeUtils, TimeUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ErDeploy

class ErSession(val sessionId: String = s"er_session_jvm_${TimeUtils.getNowMs()}_${RuntimeUtils.siteLocalAddress}",
                name: String = "",
                tag: String = "",
                createIfNotExists: Boolean = true,
                var processors: Array[ErProcessor] = Array(),
                options: Map[String, String] = Map()) {

  private var status = SessionStatus.NEW
  val clusterManagerClient = new ClusterManagerClient(options)
  private var sessionMetaArg = ErSessionMeta(
    id = sessionId,
    name=name,
    status = status,
    tag=tag,
    processors=processors,
    options=options)
  val sessionMeta: ErSessionMeta =
    if (createIfNotExists) {
      if (processors.isEmpty) clusterManagerClient.getOrCreateSession(sessionMetaArg)
      else clusterManagerClient.registerSession(sessionMetaArg)
    } else {
      clusterManagerClient.getSession(sessionMetaArg)
    }
  processors = sessionMeta.processors
  status = sessionMeta.status

  private val rolls_buffer = ArrayBuffer[ErProcessor]()
  private val eggs_buffer = mutable.Map[Long, ArrayBuffer[ErProcessor]]()
  processors.foreach(p => {
    val processorType = p.processorType
    if (processorType.toLowerCase().startsWith("egg_")) {
      eggs_buffer.getOrElseUpdate(p.serverNodeId, ArrayBuffer[ErProcessor]()) += p
    } else if (processorType.toLowerCase().startsWith("roll_")) {
      rolls_buffer += p
    } else {
      throw new IllegalArgumentException(s"processor type ${processorType} not supported in roll pair")
    }
  })

  val rolls = rolls_buffer.toArray
  val eggs : Map[Long, Array[ErProcessor]] = eggs_buffer.map(n => (n._1, n._2.toArray)).toMap

  def routeToEgg(partition: ErPartition): ErProcessor = {
    val serverNodeId = partition.processor.serverNodeId
    val eggCountOnServerNode = eggs(serverNodeId).length
    val eggIdx = partition.id / eggs.size % eggCountOnServerNode

    eggs(serverNodeId)(eggIdx)
  }
}
