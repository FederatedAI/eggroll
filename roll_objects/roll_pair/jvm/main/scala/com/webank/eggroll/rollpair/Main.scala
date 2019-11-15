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

import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.ErJob
import com.webank.eggroll.core.util.Logging
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.commons.lang3.StringUtils

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    val rollServer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 20000)).addService(new CommandService).build
    rollServer.start()

    // job
    CommandRouter.register(serviceName = RollPairService.rollMapValuesCommand,
        serviceParamTypes = Array(classOf[ErJob]),
        routeToClass = classOf[RollPairService],
        routeToMethodName = RollPairService.mapValues)

    CommandRouter.register(serviceName = RollPairService.rollReduceCommand,
        serviceParamTypes = Array(classOf[ErJob]),
        routeToClass = classOf[RollPairService],
        routeToMethodName = RollPairService.reduce)

    CommandRouter.register(serviceName = RollPairService.rollJoinCommand,
        serviceParamTypes = Array(classOf[ErJob]),
        routeToClass = classOf[RollPairService],
        routeToMethodName = RollPairService.join)

    logInfo("server started at port 20000")

    rollServer.awaitTermination()

  }
}
