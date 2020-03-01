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
import com.webank.eggroll.core.constant.SessionConfKeys
import com.webank.eggroll.core.datastructure.{RpcMessage, TaskPlan}
import com.webank.eggroll.core.meta.{ErJob, ErPartition, ErStore, ErTask}
import com.webank.eggroll.core.schedule.{BaseTaskPlan, ListScheduler}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.IdUtils

import scala.collection.mutable

class RollFrameService(session: ErSession) {
  val scheduler = new ListScheduler

  def mulMul(job: ErJob) : ErJob = {
    run(new TorchTask(uri = new CommandURI("EggFrame.runTask"),job = job))
  }

  def mapBatches(job: ErJob): ErJob = {
    // todo: deal with output
    run(new MapBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def reduce(job: ErJob): ErJob = {
    run(new ReduceBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def aggregate(job: ErJob): ErJob = {
    run(new AggregateBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def run(taskPlan: BaseTaskPlan): ErJob = {
    scheduler.addPlan(taskPlan)
    println("\nbegin run rollframe job\n")
    run(scheduler.getPlan())
    println("\nend run rollframe job\n")
    taskPlan.job
  }

  def companionEgg(): EggFrame = new EggFrame

  def run(plan: TaskPlan): Array[ErTask] = {
    val tasks = decomposeJob(taskPlan = plan)
    val commandClient = new CommandClient()
    val results = commandClient.call[ErTask](commandUri = plan.uri, args = tasks.map(t => (Array[RpcMessage](t), t.inputs.head.processor.commandEndpoint)))
    tasks
  }

  def populateProcessor(stores: Array[ErStore]): Array[ErStore] =
    stores.map(store => store.copy(partitions = store.partitions.map(partition => partition.copy(processor = session.routeToEgg(partition)))))

  def decomposeJob(taskPlan: TaskPlan): Array[ErTask] = {
    val job = taskPlan.job
    val inputStores: Array[ErStore] = job.inputs
    val partitions = inputStores.head.partitions
    val inputPartitionSize = partitions.length

    val outputStores: Array[ErStore] = job.outputs
    val result = mutable.ArrayBuffer[ErTask]()
    result.sizeHint(outputStores(0).partitions.length)

    var aggregateOutputPartition: ErPartition = null
    if (taskPlan.isAggregate) {
      aggregateOutputPartition = ErPartition(id = 0, storeLocator = outputStores.head.storeLocator, processor = session.routeToEgg(partitions(0)))
    }

    val populatedJob = if (taskPlan.shouldShuffle) {
      job.copy(
        inputs = populateProcessor(job.inputs),
        outputs = populateProcessor(job.outputs))
    } else {
      job.copy(
        inputs = Array.empty,
        outputs = Array.empty)
    }

    for (i <- 0 until inputPartitionSize) {
      val inputPartitions = mutable.ArrayBuffer[ErPartition]()
      val outputPartitions = mutable.ArrayBuffer[ErPartition]()

      inputStores.foreach(inputStore => {
        inputPartitions.append(
          ErPartition(id = i, storeLocator = inputStore.storeLocator, processor = session.routeToEgg(partitions(i))))
      })

      if (taskPlan.isAggregate) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        outputStores.foreach(outputStore => {
          outputPartitions.append(
            ErPartition(id = i, storeLocator = outputStore.storeLocator, processor = session.routeToEgg(partitions(i))))
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