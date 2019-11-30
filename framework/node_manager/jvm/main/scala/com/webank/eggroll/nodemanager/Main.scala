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

package com.webank.eggroll.nodemanager

import java.net.InetSocketAddress

import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.NodeManagerCommands
import com.webank.eggroll.core.meta.{ErProcessor, ErProcessorBatch, ErSessionMeta}
import com.webank.eggroll.core.util.{Logging, MiscellaneousUtils}
import com.webank.eggroll.nodemanager.component.NodeManagerServicer
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    val cmd = MiscellaneousUtils.parseArgs(args = args)
    val portString = cmd.getOptionValue('p', "9394")

    val rollServer = NettyServerBuilder
      .forAddress(new InetSocketAddress("127.0.0.1", portString.toInt))
      .addService(new CommandService).build

    CommandRouter.register(serviceName = NodeManagerCommands.getOrCreateProcessorBatchServiceName,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[NodeManagerServicer],
      routeToMethodName = NodeManagerCommands.getOrCreateProcessorBatch)

    CommandRouter.register(serviceName = NodeManagerCommands.getOrCreateServicerServiceName,
      serviceParamTypes = Array(classOf[ErSessionMeta]),
      serviceResultTypes = Array(classOf[ErProcessorBatch]),
      routeToClass = classOf[NodeManagerServicer],
      routeToMethodName = NodeManagerCommands.getOrCreateServicer)

    CommandRouter.register(serviceName = NodeManagerCommands.heartbeatServiceName,
      serviceParamTypes = Array(classOf[ErProcessor]),
      serviceResultTypes = Array(classOf[ErProcessor]),
      routeToClass = classOf[NodeManagerServicer],
      routeToMethodName = NodeManagerCommands.heartbeat)
    rollServer.start()
    val port = rollServer.getPort

    val msg = s"server started at ${port}"
    println(msg)
    logInfo(msg)
  }
}
