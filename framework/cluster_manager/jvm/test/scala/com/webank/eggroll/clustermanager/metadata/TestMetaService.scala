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

package com.webank.eggroll.clustermanager.metadata

import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.meta.ErServerNode
import com.webank.eggroll.core.transfer.GrpcTransferService
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.junit.Test

class TestMetaService {
  @Test
  def testGetNode(): Unit = {
    CommandRouter.register(serviceName = "cluster-manager/meta-service/v1/getNode",
      serviceParamTypes = Array(classOf[ErServerNode]),
      serviceResultTypes = Array(classOf[ErServerNode]),
      routeToClass = classOf[NodeCrudOperator])

    val metaService = NettyServerBuilder
      .forPort(20001)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()


  }

}
