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
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, StoreTypes}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.GrpcTransferService
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.rollpair.component.{EggPair, RollPairServicer}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.junit.Test


class TestRollPair extends Logging {
  StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, "localhost")
  StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, "4670")
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
    CommandRouter.register(serviceName = RollPairServicer.rollMapValuesCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.mapValues)

    val eggServer = NettyServerBuilder.forPort(20001).addService(new CommandService).build()
    eggServer.start()

    // task
    CommandRouter.register(serviceName = RollPairServicer.eggMapValuesCommand,
      serviceParamTypes = Array(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairServicer.runTask)


    val storeLocator = ErStoreLocator(StoreTypes.ROLLPAIR_LEVELDB, "namespace", "name")

    val rollPair = new RollPairServicer()

    val f: Array[Byte] => Array[Byte] = appendByte

    val job = ErJob(id = "1",
      name = "mapValues",
      inputs = Array(ErStore(storeLocator)),
      outputs = Array(ErStore(ErStoreLocator(storeType = StoreTypes.ROLLPAIR_LEVELDB, namespace = "namespace", name = "testMapValues"))),
      functors = Array(ErFunctor("mapValues", "",
        RollPairServicer.functorSerDes.serialize(f))))

    val result = rollPair.mapValues(job)
    println(result)
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
    CommandRouter.register(serviceName = RollPairServicer.rollMapCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.map)

    val eggServer = NettyServerBuilder.forPort(20001)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()
    eggServer.start()

    // task
    CommandRouter.register(serviceName = RollPairServicer.eggMapCommand,
      serviceParamTypes = Array(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairServicer.runTask)


    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPairServicer()

    val f: (Array[Byte], Array[Byte]) => (Array[Byte], Array[Byte]) = prependToBoth
    val p: Array[Byte] => Int = partitioner

    val job = ErJob(id = "1",
      name = "map",
      inputs = Array(ErStore(storeLocator)),
      functors = Array(ErFunctor("map", "",
        RollPairServicer.functorSerDes.serialize(f)), ErFunctor("map", "", RollPairServicer.functorSerDes.serialize(p))))

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
    CommandRouter.register(serviceName = RollPairServicer.rollReduceCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.reduce)

    val eggServer = NettyServerBuilder
      .forPort(20001)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()
    eggServer.start()

    // task
    CommandRouter.register(serviceName = RollPairServicer.eggReduceCommand,
      serviceParamTypes = Array(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairServicer.runTask)

    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPairServicer()

    val f: (Array[Byte], Array[Byte]) => Array[Byte] = concat

    val job = ErJob(id = "1",
      name = "reduce",
      inputs = Array(ErStore(storeLocator)),
      functors = Array(ErFunctor("reduce", "",
        RollPairServicer.functorSerDes.serialize(f))))


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

    CommandRouter.register(serviceName = RollPairServicer.rollRunJobCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)

    val eggServer = NettyServerBuilder.forPort(20001).addService(new CommandService).build()
    eggServer.start()
    CommandRouter.register(serviceName = RollPairServicer.eggJoinCommand,
      serviceParamTypes = Array(classOf[ErTask]),
      routeToClass = classOf[EggPair],
      routeToMethodName = RollPairServicer.runTask)


    val leftLocator = ErStoreLocator(StoreTypes.ROLLPAIR_LEVELDB, "namespace", "name")
    val rightLocator = ErStoreLocator(StoreTypes.ROLLPAIR_LEVELDB, "namespace", "test")

    val rollPair = new RollPairServicer()

    val f: (Array[Byte], Array[Byte]) => Array[Byte] = concat

    val job = ErJob(id = "1",
      name = "join",
      inputs = Array(ErStore(leftLocator), ErStore(rightLocator)),
      functors = Array(ErFunctor("join", "",
        RollPairServicer.functorSerDes.serialize(f))))

    val result = rollPair.runJob(job)
  }

  @Test
  def startRollPairAsService(): Unit = {
    val rollServer = NettyServerBuilder.forPort(20000).addService(new CommandService).build
    rollServer.start()

    // job
    CommandRouter.register(serviceName = RollPairServicer.rollMapValuesCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.mapValues)

    CommandRouter.register(serviceName = RollPairServicer.rollMapCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.map)

    CommandRouter.register(serviceName = RollPairServicer.rollReduceCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.reduce)

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

    CommandRouter.register(serviceName = RollPairServicer.rollJoinCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.join)

    CommandRouter.register(serviceName = RollPairServicer.rollRunJobCommand,
      serviceParamTypes = Array(classOf[ErJob]),
      routeToClass = classOf[RollPairServicer],
      routeToMethodName = RollPairServicer.runJob)

    logInfo("started")
    Thread.sleep(12000000)
  }
}
