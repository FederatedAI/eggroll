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

package com.webank.eggroll.rollpair.component

import com.webank.eggroll.core.command.{CollectiveCommand, CommandURI}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.serdes.DefaultScalaFunctorSerdes

class RollPair() {
  def mapValues(inputJob: ErJob): ErStore = {
    // f: Array[Byte] => Array[Byte]
    // val functor = ErFunctor("map_user_defined", functorSerDes.serialize(f))

    val inputStore = inputJob.inputs.head
    val inputLocator = inputStore.storeLocator
    val outputLocator = inputLocator.copy(name = "testoutput")

    val inputPartition = ErPartition(id = "0", storeLocator = inputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))
    val outputPartition = ErPartition(id = "0", storeLocator = outputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))

    // todo: move to cluster manager
    val inputStoreWithPartitions = inputStore.copy(storeLocator = inputLocator,
      partitions = List(inputPartition.copy(id = "0"), inputPartition.copy(id = "1")))

    val outputStoreWithPartitions = inputStore.copy(storeLocator = outputLocator,
      partitions = List(outputPartition.copy(id = "0"), outputPartition.copy(id = "1")))

    val job = inputJob.copy(inputs = List(inputStoreWithPartitions), outputs = List(outputStoreWithPartitions))

    val collectiveCommand = CollectiveCommand(new CommandURI(RollPair.eggMapCommand), job)

    // todo: update database
    // val xxx = updateOutputDb()

    val commandResults = collectiveCommand.call()

    outputStoreWithPartitions
  }

  def reduce(inputJob: ErJob): ErStore = {
    val inputStore = inputJob.inputs.head
    val inputLocator = inputStore.storeLocator
    val outputLocator = inputLocator.copy(name = "testReduce")

    val inputPartition = ErPartition(id = "0", storeLocator = inputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))
    val outputPartition = ErPartition(id = "0", storeLocator = outputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))

    val inputStoreWithPartitions = inputStore.copy(storeLocator = inputLocator,
      partitions = List(inputPartition.copy(id = "0"), inputPartition.copy(id = "1")))

    val outputStoreWithPartitions = inputStore.copy(storeLocator = outputLocator,
      partitions = List(outputPartition))

    val job = inputJob.copy(inputs = List(inputStoreWithPartitions), outputs = List(outputStoreWithPartitions))

    val collectiveCommand = CollectiveCommand(new CommandURI(RollPair.eggReduceCommand), job)

    val commandResults = collectiveCommand.call()

    outputStoreWithPartitions
  }
}

object RollPair {
  val clazz = classOf[RollPair]
  val functorSerDes = DefaultScalaFunctorSerdes()

  val mapValues = "mapValues"
  val reduce = "reduce"
  val eggReduceCommand = classOf[EggPair].getCanonicalName + StringConstants.DOT + reduce
  var rollMapCommand = clazz.getCanonicalName + StringConstants.DOT + mapValues
  var eggMapCommand = classOf[EggPair].getCanonicalName + StringConstants.DOT + mapValues
  var rollReduceCommand = classOf[RollPair].getCanonicalName + StringConstants.DOT + reduce

  /*  CommandRouter.register(mapCommand,
      List(classOf[Array[Byte] => Array[Byte]]), clazz, "mapValues", null, null)*/
}