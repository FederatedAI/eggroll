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

package com.webank.eggroll.rollpair

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import _root_.io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{ProcessorStatus, ProcessorTypes, SessionConfKeys}
import com.webank.eggroll.core.meta.{ErEndpoint, ErJob, ErProcessor}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{Logging, MiscellaneousUtils}
import com.webank.eggroll.rollpair.component.RollPairServicer


object Main extends Logging {
  def registerRouter():Unit = {
    CommandRouter.register(serviceName = RollPairServicer.rollMapValuesCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.mapValues)

    CommandRouter.register(serviceName = RollPairServicer.rollMapCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)

    CommandRouter.register(serviceName = RollPairServicer.rollMapPartitionsCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.mapPartitions)

    CommandRouter.register(serviceName = RollPairServicer.rollCollapsePartitionsCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.collapsePartitions)

    CommandRouter.register(serviceName = RollPairServicer.rollFlatMapCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.flatMap)

    CommandRouter.register(serviceName = RollPairServicer.rollGlomCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.glom)

    CommandRouter.register(serviceName = RollPairServicer.rollSampleCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.sample)

    CommandRouter.register(serviceName = RollPairServicer.rollFilterCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.filter)

    CommandRouter.register(serviceName = RollPairServicer.rollSubtractByKeyCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.subtractByKey)

    CommandRouter.register(serviceName = RollPairServicer.rollUnionCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.union)

    CommandRouter.register(serviceName = RollPairServicer.rollReduceCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.reduce)

    CommandRouter.register(serviceName = RollPairServicer.rollJoinCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)

    CommandRouter.register(serviceName = RollPairServicer.rollRunJobCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)

    CommandRouter.register(serviceName = RollPairServicer.rollPutAllCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)

    CommandRouter.register(serviceName = RollPairServicer.rollGetAllCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)
  }
  def reportCM(sessionId:String, args: Array[String], myCommandPort: Int):Unit = {
    val cmd = MiscellaneousUtils.parseArgs(args)
    // todo:2: heartbeat service
    val portString = cmd.getOptionValue('p', "0")
    val sessionId = cmd.getOptionValue('s', "UNKNOWN")
    val clusterManager = cmd.getOptionValue("cluster-manager")
    val nodeManager = cmd.getOptionValue("node-manager")
    val serverNodeId = cmd.getOptionValue("server-node-id").toLong
    val processorId = cmd.getOptionValue("processor-id").toLong

    val clusterManagerClient = new ClusterManagerClient(ErEndpoint(clusterManager))
    val options = new ConcurrentHashMap[String, String]()
    options.put(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)

    val myself = ErProcessor(
      id = processorId,
      serverNodeId = serverNodeId,
      processorType = ProcessorTypes.ROLL_PAIR_MASTER,
      commandEndpoint = ErEndpoint("localhost", myCommandPort),
      transferEndpoint = ErEndpoint("localhost", myCommandPort),
      options = options,
      status = ProcessorStatus.RUNNING)

    logInfo("ready to heartbeat")
    clusterManagerClient.heartbeat(myself)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logInfo(s"*** roll pair master exit gracefully. sessionId: ${sessionId}, serverNodeId: ${serverNodeId}, processorId: ${processorId}, port: ${portString} ***")
        val terminatedSelf = myself.copy(status = ProcessorStatus.STOPPED)
        clusterManagerClient.heartbeat(terminatedSelf)
        this.interrupt()
      }
    })
  }
  def main(args: Array[String]): Unit = {
    registerRouter()
    val cmd = MiscellaneousUtils.parseArgs(args = args)
    val portString = cmd.getOptionValue('p', "0")
    val sessionId = cmd.getOptionValue('s', "UNKNOWN")

    val rollServer = NettyServerBuilder
      .forAddress(new InetSocketAddress("127.0.0.1", portString.toInt))
      .addService(new CommandService)
      .build
    rollServer.start()
    val port = rollServer.getPort
    StaticErConf.setPort(port)
    StaticErConf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    logInfo(s"server started at ${port}")
    // job
    reportCM(sessionId, args, port)

    logInfo("heartbeated")
    rollServer.awaitTermination()
  }
}
