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

package com.webank.eggroll.rollframe

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.IdUtils

import scala.collection.mutable


class RollFrameScheduler(session: ErSession) {

  def run(job: ErJob, isAggregate:Boolean = false, shouldShuffle:Boolean = false): Array[ErPair] = {
    val tasks = decomposeJob(job, isAggregate, shouldShuffle)
    val commandClient = new CommandClient()
    val uri = new CommandURI("EggFrame.runTask")
    commandClient.call[ErPair](
      commandUri = uri,
      args = tasks.map(t => (Array[RpcMessage](t), t.inputs.head.processor.commandEndpoint)))
  }

  def populateProcessor(stores: Array[ErStore]): Array[ErStore] =
    stores.map(store => store.copy(partitions =
      store.partitions.map(partition =>
        partition.copy(processor = session.routeToEgg(partition)))))

  def decomposeJob(job: ErJob, isAggregate:Boolean, shouldShuffle:Boolean): Array[ErTask] = {
    val inputStores: Array[ErStore] = job.inputs
    val partitions = inputStores.head.partitions
    val inputPartitionSize = partitions.length

    val outputStores: Array[ErStore] = job.outputs
    val result = mutable.ArrayBuffer[ErTask]()
    result.sizeHint(outputStores(0).partitions.length)

    var aggregateOutputPartition: ErPartition = null
    if (isAggregate) {
      aggregateOutputPartition = ErPartition(id = 0, storeLocator = outputStores.head.storeLocator,
        processor = session.routeToEgg(partitions(0)))
    }
    val populatedJob = if (shouldShuffle) {
      // TODO:2: check populated
      job.copy(
        inputs = populateProcessor(job.inputs),
        outputs = populateProcessor(job.outputs))
    } else {
      // TODO:2: reduce task rpc data size
      job
    }

    for (i <- 0 until inputPartitionSize) {
      val inputPartitions = mutable.ArrayBuffer[ErPartition]()
      val outputPartitions = mutable.ArrayBuffer[ErPartition]()

      inputStores.foreach(inputStore => {
        inputPartitions.append(
          ErPartition(id = i, storeLocator = inputStore.storeLocator, processor = partitions(i).processor))
      })

      if (isAggregate) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        outputStores.foreach(outputStore => {
          outputPartitions.append(
            ErPartition(id = i, storeLocator = outputStore.storeLocator, processor = partitions(i).processor))
        })
      }

      result.append(
        ErTask(
          id = IdUtils.generateTaskId(job.id, i),
          name = job.name,
          inputs = inputPartitions.toArray,
          outputs = outputPartitions.toArray,
          job = populatedJob))
    }

    result.toArray
  }
}