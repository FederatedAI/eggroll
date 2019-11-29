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

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.{ProcessorStatus, ProcessorTypes, SessionConfKeys}
import com.webank.eggroll.core.meta.{ErEndpoint, ErJob, ErProcessor}
import com.webank.eggroll.core.util.{Logging, MiscellaneousUtils}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    logInfo("going into main")
    val cmd = MiscellaneousUtils.parseArgs(args = args)
    val portString = cmd.getOptionValue('p', "0")
    val sessionId = cmd.getOptionValue('s')

    val rollServer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", portString.toInt)).addService(new CommandService).build
    rollServer.start()
    val port = rollServer.getPort

    logInfo(s"server started at ${port}")
    // job
    CommandRouter.register(serviceName = RollPairServicer.rollMapValuesCommand,
        serviceParamTypes = Array(classOf[ErJob]),
        routeToClass = classOf[RollPairServicer],
        routeToMethodName = RollPairServicer.mapValues)

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

    logInfo("server started at port 20000")


    // todo: get port from command line
    // todo: heartbeat service
    val nodeManagerClient = new NodeManagerClient()
    val options = new ConcurrentHashMap[String, String]()
    options.put(SessionConfKeys.CONFKEY_SESSION_ID, sessionId)
    val myself = ErProcessor(
      processorType = ProcessorTypes.ROLL_PAIR_SERVICER,
      commandEndpoint = ErEndpoint("localhost", port),
      dataEndpoint = ErEndpoint("localhost", port),
      options = options,
      status = ProcessorStatus.RUNNING)

    logInfo("ready to heartbeat")
    nodeManagerClient.heartbeat(myself)

    logInfo("heartbeated")
    rollServer.awaitTermination()

  }
}
