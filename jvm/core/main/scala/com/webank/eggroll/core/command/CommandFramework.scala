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

package com.webank.eggroll.core.command

import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.{SerdesTypes, SessionConfKeys}
import com.webank.eggroll.core.datastructure.TaskPlan
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{Logging, ThreadPoolUtils}

import scala.collection.mutable

case class EndpointCommand(commandURI: CommandURI, job: ErJob)

case class EndpointTaskCommand(commandURI: CommandURI, task: ErTask)


// todo: merge with command client
case class CollectiveCommand(taskPlan: TaskPlan) extends Logging {
  def call(): Array[ErTask] = {
    val job = taskPlan.job

    val commandUri = taskPlan.uri

    val errors = new DistributedRuntimeException()
    val results = mutable.ArrayBuffer[ErTask]()

    val tasks = toTasks(taskPlan)

    val future = CompletableFuture.allOf(
      tasks.map(task => {
        CompletableFuture
          .supplyAsync(new CommandServiceSupplier(task, commandUri), CollectiveCommand.threadPool)
          .exceptionally(e => {
            errors.append(e)
            null
          })
          .whenCompleteAsync((result, exception) => {
            if (exception != null) {
              errors.append(exception)
            } else {
              // nothing
            }
          })
      }): _*).whenComplete((result, exception) => {
        if (exception != null) {
          errors.append(exception)
        } else {
          // nothing
        }
      })

    future.join()

    if (!errors.check()) {
      errors.raise()
    }
    results.toArray
  }

  def populateProcessor(stores: Array[ErStore]): Array[ErStore] =
    stores.map(store => store.copy(partitions = store.partitions.map(partition => partition.copy(processor = CollectiveCommand.session.routeToEgg(partition)))))

  def toTasks(taskPlan: TaskPlan): Array[ErTask] = {
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
      aggregateOutputPartition = ErPartition(id = 0, storeLocator = outputStores.head.storeLocator, processor = CollectiveCommand.session.routeToEgg(partitions(0)))
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
          ErPartition(id = i, storeLocator = inputStore.storeLocator, processor = CollectiveCommand.session.routeToEgg(partitions(i))))
      })

      if (taskPlan.isAggregate) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        outputStores.foreach(outputStore => {
          outputPartitions.append(
            ErPartition(id = i, storeLocator = outputStore.storeLocator, processor = CollectiveCommand.session.routeToEgg(partitions(i))))
        })
      }

      result.append(
        ErTask(
          id = s"${job.id}-${i}",
          name = job.name,
          inputs = inputPartitions.toArray,
          outputs = outputPartitions.toArray,
          job = populatedJob))
    }

    result.toArray
  }
}

class CommandServiceSupplier(task: ErTask, command: CommandURI)
  extends Supplier[ErTask] {
  override def get(): ErTask = {
    val client = new CommandClient()
    //client.sendTask(task, command)
    client.simpleSyncSend(
      input = task,
      outputType = classOf[ErTask],
      endpoint = task.getCommandEndpoint,
      commandURI = command,
      serdesType = SerdesTypes.PROTOBUF).asInstanceOf[ErTask]
  }
}

object CollectiveCommand {
  val threadPool = ThreadPoolUtils.newFixedThreadPool(20, "command-")
  val session = new ErSession(StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_ID))
}
