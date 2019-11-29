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

package com.webank.eggroll.clustermanager.session

import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{NodeTypes, ProcessorTypes, ServerNodeStatus, SessionConfKeys}
import com.webank.eggroll.core.datastructure.RollContext
import com.webank.eggroll.core.meta.{ErServerNode, ErSession, ErSessionMeta}
import com.webank.eggroll.core.schedule.deploy.JvmProcessorOperator
import com.webank.eggroll.core.session.RuntimeErConf
import com.webank.eggroll.rollpair.component.RollPairContext

trait SessionDeployer {
  def deploy(session: ErSessionMeta): ErSession
}


class ClusterSessionDeployer extends SessionDeployer {
  private val clusterManagerClient = new ClusterManagerClient()

  override def deploy(sessionMeta: ErSessionMeta): ErSession = {
    val healtyNodeManagerExample = ErServerNode(status = ServerNodeStatus.HEALTHY, nodeType = NodeTypes.NODE_MANAGER)

    // when cluster is introduced, this needs change to select a specific cluster
    val serverCluster = clusterManagerClient.getServerNodes(healtyNodeManagerExample)

    val sessionConf = new RuntimeErConf(sessionMeta.options)

    // todo: support more context types
    val rollPairCount = sessionConf.getInt(SessionConfKeys.CONFKEY_SESSION_CONTEXT_ROLLPAIR_COUNT, 1)

    val rollPairContext = new RollPairContext(sessionMeta)

    val contexts = new ConcurrentHashMap[String, RollContext]()
    contexts.put(ProcessorTypes.ROLL_PAIR, rollPairContext)
    sessionMeta.toErSession().copy(contexts = contexts, serverCluster = serverCluster)
  }
}
