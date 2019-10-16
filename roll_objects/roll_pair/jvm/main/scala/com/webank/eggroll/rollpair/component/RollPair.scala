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
import com.webank.eggroll.core.meta.MetaModelPbSerdes._
import com.webank.eggroll.core.serdes.DefaultScalaFunctorSerdes

import scala.collection.mutable

class RollPair() {
  def mapValues(inputJob: ErJob): ErStore = {
    // f: Array[Byte] => Array[Byte]
    // val functor = ErFunctor("map_user_defined", functorSerDes.serialize(f))

    val inputStore = inputJob.inputs.head
    val inputLocator = inputStore.storeLocator
    val outputLocator = inputLocator.copy(name = "testMapValues")

    val inputPartitionTemplate = ErPartition(id = "0", storeLocator = inputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))
    val outputPartitionTemplate = ErPartition(id = "0", storeLocator = outputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))

    val numberOfPartitions = 4

    val inputPartitions = mutable.ListBuffer[ErPartition]()
    val outputPartitions = mutable.ListBuffer[ErPartition]()

    for (i <- 0 until numberOfPartitions) {
      inputPartitions += inputPartitionTemplate.copy(id = i.toString)
      outputPartitions += outputPartitionTemplate.copy(id = i.toString)
    }

    // todo: move to cluster manager
    val inputStoreWithPartitions = inputStore.copy(storeLocator = inputLocator,
      partitions = inputPartitions.toList)
    val outputStoreWithPartitions = inputStore.copy(storeLocator = outputLocator,
      partitions = outputPartitions.toList)

    val job = inputJob.copy(inputs = List(inputStoreWithPartitions), outputs = List(outputStoreWithPartitions))

    val collectiveCommand = CollectiveCommand(new CommandURI(RollPair.eggMapValuesCommand), job)

    // todo: update database
    // val xxx = updateOutputDb()

    val commandResults = collectiveCommand.call()

    outputStoreWithPartitions
  }

  def map(inputJob: ErJob): ErStore = {
    val inputStore = inputJob.inputs.head
    val inputLocator = inputStore.storeLocator
    val outputLocator = inputLocator.copy(name = "testMap")

    val inputPartitionTemplate = ErPartition(id = "0", storeLocator = inputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))
    val outputPartitionTemplate = ErPartition(id = "0", storeLocator = outputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))

    val numberOfPartitions = 4

    val inputPartitions = mutable.ListBuffer[ErPartition]()
    val outputPartitions = mutable.ListBuffer[ErPartition]()

    for (i <- 0 until numberOfPartitions) {
      inputPartitions += inputPartitionTemplate.copy(id = i.toString)
      outputPartitions += outputPartitionTemplate.copy(id = i.toString)
    }

    val inputStoreWithPartitions = inputStore.copy(storeLocator = inputLocator,
      partitions = inputPartitions.toList)
    val outputStoreWithPartitions = inputStore.copy(storeLocator = outputLocator,
      partitions = outputPartitions.toList)

    val job = inputJob.copy(inputs = List(inputStoreWithPartitions), outputs = List(outputStoreWithPartitions))

    val collectiveCommand = CollectiveCommand(new CommandURI(RollPair.eggMapValuesCommand), job)

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

  def join(inputJob: ErJob): ErStore = {
    val leftStore = inputJob.inputs.head
    val leftLocator = leftStore.storeLocator

    val rightStore = inputJob.inputs(1)
    val rightLocator = rightStore.storeLocator

    val outputLocator = leftLocator.copy(name = "testJoin")

    val leftPartitionTemplate = ErPartition(id = "0", storeLocator = leftLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))
    val rightPartitionTemplate = ErPartition(id = "0", storeLocator = rightLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))
    val outputPartitionTemplate = ErPartition(id = "0", storeLocator = outputLocator, node = ErServerNode(endpoint = ErEndpoint("localhost", 20001)))

    val numberOfPartitions = 4

    val leftPartitions = mutable.ListBuffer[ErPartition]()
    val rightPartitions = mutable.ListBuffer[ErPartition]()
    val outputPartitions = mutable.ListBuffer[ErPartition]()

    for (i <- 0 until numberOfPartitions) {
      leftPartitions += leftPartitionTemplate.copy(id = i.toString)
      rightPartitions += rightPartitionTemplate.copy(id = i.toString)
      outputPartitions += outputPartitionTemplate.copy(id = i.toString)
    }

    val leftStoreWithPartitions = leftStore.copy(partitions = leftPartitions.toList)
    val rightStoreWithPartitions = rightStore.copy(partitions = rightPartitions.toList)
    val outputStoreWithPartitions = ErStore(storeLocator = outputLocator, partitions = outputPartitions.toList)

    val job = inputJob.copy(inputs = List(leftStoreWithPartitions, rightStoreWithPartitions), outputs = List(outputStoreWithPartitions))
    val collectiveCommand = CollectiveCommand(new CommandURI(RollPair.eggJoinCommand), job)

    val commandResults = collectiveCommand.call()

    outputStoreWithPartitions
  }
}

object RollPair {
  val clazz = classOf[RollPair]
  val functorSerDes = DefaultScalaFunctorSerdes()

  val map = "map"
  val mapValues = "mapValues"
  val reduce = "reduce"
  val join = "join"
  val rollPair = "RollPair"
  val eggPair = "EggPair"

  var eggMapCommand = s"${eggPair}.${map}"
  var eggMapValuesCommand = s"${eggPair}.${mapValues}"
  val eggReduceCommand = s"${eggPair}.${reduce}"
  var eggJoinCommand = s"${eggPair}.${join}"

  var rollMapCommand = s"${rollPair}.${map}"
  var rollMapValuesCommand = s"${rollPair}.${mapValues}"
  var rollReduceCommand = s"${rollPair}.${reduce}"
  var rollJoinCommand = s"${rollPair}.${join}"

  /*  CommandRouter.register(mapCommand,
      List(classOf[Array[Byte] => Array[Byte]]), clazz, "mapValues", null, null)*/
}