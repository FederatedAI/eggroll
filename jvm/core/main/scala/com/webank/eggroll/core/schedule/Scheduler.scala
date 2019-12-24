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

package com.webank.eggroll.core.schedule

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.CommandClient
import com.webank.eggroll.core.constant.SessionConfKeys
import com.webank.eggroll.core.datastructure.{RpcMessage, TaskPlan}
import com.webank.eggroll.core.meta.{ErJob, ErPartition, ErStore, ErTask}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{IdUtils, Logging}

import scala.collection.mutable

trait Scheduler extends Logging {

}



// todo: add another layer of abstraction if coupling with ErJob is proved a bad practice or
//  communication (e.g. broadcast), or io operation should be described in task plan as computing
case class ListScheduler() extends Scheduler {
  private val stages = mutable.Queue[TaskPlan]()
  private val functorSerdes = DefaultScalaSerdes()

  def addPlan(plan: TaskPlan): ListScheduler = {
    stages += plan
    this
  }

  def getPlan(): TaskPlan = {
    stages.dequeue()
  }

}

object JobRunner {
  // TODO:1: global session info?
  val session = new ErSession(StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_ID))

  def run(plan: TaskPlan): Array[ErTask] = {
    val tasks = decomposeJob(taskPlan = plan)


    val commandClient = new CommandClient()

    val results = commandClient.call[ErTask](commandURI = plan.uri, args = tasks.map(t => (Array[RpcMessage](t), t.inputs.head.processor.commandEndpoint)))
    tasks
  }

  def populateProcessor(stores: Array[ErStore]): Array[ErStore] =
    stores.map(store => store.copy(partitions = store.partitions.map(partition => partition.copy(processor = session.routeToEgg(partition)))))

  def decomposeJob(taskPlan: TaskPlan): Array[ErTask] = {
    val job = taskPlan.job
    val inputStores: Array[ErStore] = job.inputs
    val inputPartitionSize = inputStores.head.storeLocator.totalPartitions
    val inputOptions = job.options

    val partitions = inputStores.head.partitions

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
      ErJob(id = job.id, name = job.name, inputs = Array.empty, outputs = Array.empty, functors = job.functors)
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