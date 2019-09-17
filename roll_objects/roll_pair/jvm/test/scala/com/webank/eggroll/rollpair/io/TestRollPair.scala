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
 */

package com.webank.eggroll.rollpair.io

import com.webank.eggroll.core.command.{CommandRouter, CommandService}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.transfer.GrpcTransferService
import com.webank.eggroll.rollpair.component.{EggPair, RollPair}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.commons.lang3.StringUtils
import org.junit.Test

class TestRollPair {
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
    val mapValuesMethod = classOf[RollPair].getMethod("mapValues", classOf[ErJob])
    val mapValuesServiceName = String.join(
      StringConstants.DOT,
      StringUtils.strip(mapValuesMethod.getDeclaringClass.getCanonicalName, StringConstants.DOLLAR),
      mapValuesMethod.getName)
    CommandRouter.register(mapValuesServiceName, List(classOf[ErJob]))

    val eggServer = NettyServerBuilder.forPort(20001).addService(new CommandService).build()
    eggServer.start()

    // task
    val mapValuesEggMethod = classOf[EggPair].getMethod("mapValues", classOf[ErTask])
    val mapValuesEggServiceName = String.join(
      StringConstants.DOT,
      StringUtils.strip(mapValuesEggMethod.getDeclaringClass.getCanonicalName, StringConstants.DOLLAR),
      mapValuesEggMethod.getName)
    CommandRouter.register(mapValuesEggServiceName, List(classOf[ErTask]))


    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPair()

    val f: Array[Byte] => Array[Byte] = appendByte

    val job = ErJob(id = "1",
      name = "mapValues",
      inputs = List(ErStore(storeLocator)),
      functors = List(ErFunctor("mapValues",
        RollPair.functorSerDes.serialize(f))))


    val result = rollPair.mapValues(job)
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
    val mapValuesMethod = classOf[RollPair].getMethod("reduce", classOf[ErJob])
    val mapValuesServiceName = String.join(
      StringConstants.DOT,
      StringUtils.strip(mapValuesMethod.getDeclaringClass.getCanonicalName, StringConstants.DOLLAR),
      mapValuesMethod.getName)
    CommandRouter.register(mapValuesServiceName, List(classOf[ErJob]))

    val eggServer = NettyServerBuilder
      .forPort(20001)
      .addService(new CommandService)
      .addService(new GrpcTransferService)
      .build()
    eggServer.start()

    // task
    val mapValuesEggMethod = classOf[EggPair].getMethod("reduce", classOf[ErTask])
    val mapValuesEggServiceName = String.join(
      StringConstants.DOT,
      StringUtils.strip(mapValuesEggMethod.getDeclaringClass.getCanonicalName, StringConstants.DOLLAR),
      mapValuesEggMethod.getName)
    CommandRouter.register(mapValuesEggServiceName, List(classOf[ErTask]))


    val storeLocator = ErStoreLocator("levelDb", "ns", "name")

    val rollPair = new RollPair()

    val f: (Array[Byte], Array[Byte]) => Array[Byte] = concat

    val job = ErJob(id = "1",
      name = "reduce",
      inputs = List(ErStore(storeLocator)),
      functors = List(ErFunctor("reduce",
        RollPair.functorSerDes.serialize(f))))


    val result = rollPair.reduce(job)
  }
}
