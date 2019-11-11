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

package com.webank.eggroll.rollpair.io

import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.transfer.GrpcTransferService
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.rollpair.component.{EggPair, RollPairService}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.junit.Test


class TestRollPair extends Logging {
  @Test
  def testMapValues(): Unit = {
    def append(value: String): String = {
      value + "1"
    }

    def appendByte(value: Array[Byte]): Array[Byte] = {
      value ++ "2".getBytes
    }

    val rollServer = NettyServerBuilder.forPort(20000).addService(new CommandService).build
    rollServer.start()

    // job
    CommandRouter.register(serviceName = RollPairService.rollMapValuesCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.mapValues)

    val eggServer = NettyServerBuilder.forPort(20001).addService(new CommandService).build()
    eggServer.start()

    // task
    CommandRouter.register(serviceName = RollPairService.eggMapValuesCommand,
      serviceParamTypes = List(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairService.runTask)


    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPairService()

    val f: Array[Byte] => Array[Byte] = appendByte

    val job = ErJob(id = "1",
      name = "mapValues",
      inputs = List(ErStore(storeLocator)),
      functors = List(ErFunctor("mapValues", "",
        RollPairService.functorSerDes.serialize(f))))


    val result = rollPair.mapValues(job)
  }

  @Test
  def testMap(): Unit = {
    def prependToBoth(key: Array[Byte], value: Array[Byte]): (Array[Byte], Array[Byte]) = {
      ("k_".getBytes() ++ key, "v_".getBytes() ++ value)
    }

    def partitioner(key: Array[Byte]): Int = {
      key.last % 4
    }

    val rollServer = NettyServerBuilder.forPort(20000)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build
    rollServer.start()

    // job
    CommandRouter.register(serviceName = RollPairService.rollMapCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.map)

    val eggServer = NettyServerBuilder.forPort(20001)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()
    eggServer.start()

    // task
    CommandRouter.register(serviceName = RollPairService.eggMapCommand,
      serviceParamTypes = List(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairService.runTask)


    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPairService()

    val f: (Array[Byte], Array[Byte]) => (Array[Byte], Array[Byte]) = prependToBoth
    val p: Array[Byte] => Int = partitioner

    val job = ErJob(id = "1",
      name = "map",
      inputs = List(ErStore(storeLocator)),
      functors = List(ErFunctor("map", "",
        RollPairService.functorSerDes.serialize(f)), ErFunctor("map", "", RollPairService.functorSerDes.serialize(p))))

    val result = rollPair.map(job)
  }

  @Test
  def testReduce(): Unit = {
    def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
      a ++ b
    }

    val rollServer = NettyServerBuilder.forPort(20000)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build
    rollServer.start()

    // job
    CommandRouter.register(serviceName = RollPairService.rollReduceCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.reduce)

    val eggServer = NettyServerBuilder
      .forPort(20001)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()
    eggServer.start()

    // task
    CommandRouter.register(serviceName = RollPairService.eggReduceCommand,
      serviceParamTypes = List(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairService.runTask)

    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPairService()

    val f: (Array[Byte], Array[Byte]) => Array[Byte] = concat

    val job = ErJob(id = "1",
      name = "reduce",
      inputs = List(ErStore(storeLocator)),
      functors = List(ErFunctor("reduce", "",
        RollPairService.functorSerDes.serialize(f))))


    val result = rollPair.reduce(job)
  }

  @Test
  def testJoin(): Unit = {
    def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
      a ++ b
    }

    val rollServer = NettyServerBuilder.forPort(20000)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build
    rollServer.start()

    CommandRouter.register(serviceName = RollPairService.rollJoinCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.join)

    val eggServer = NettyServerBuilder.forPort(20001).addService(new CommandService).build()
    eggServer.start()
    CommandRouter.register(serviceName = RollPairService.eggJoinCommand,
      serviceParamTypes = List(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairService.runTask)


    val leftLocator = ErStoreLocator("levelDb", "ns", "name")
    val rightLocator = ErStoreLocator("levelDb", "ns", "test")

    val rollPair = new RollPairService()

    val f: (Array[Byte], Array[Byte]) => Array[Byte] = concat

    val job = ErJob(id = "1",
      name = "join",
      inputs = List(ErStore(leftLocator), ErStore(rightLocator)),
      functors = List(ErFunctor("join", "",
        RollPairService.functorSerDes.serialize(f))))

    val result = rollPair.join(job)
  }

  @Test
  def startRollPairAsService(): Unit = {
    val rollServer = NettyServerBuilder.forPort(20000).addService(new CommandService).build
    rollServer.start()

    // job
    CommandRouter.register(serviceName = RollPairService.rollMapValuesCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.mapValues)

    CommandRouter.register(serviceName = RollPairService.rollMapCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.map)

    CommandRouter.register(serviceName = RollPairService.rollReduceCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.reduce)

    CommandRouter.register(serviceName = RollPairService.rollJoinCommand,
      serviceParamTypes = List(classOf[ErJob]),
      routeToClass = classOf[RollPairService],
      routeToMethodName = RollPairService.join)


    logInfo("started")
    Thread.sleep(1200000)
  }
}
