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

import java.util.concurrent.{CompletableFuture, CountDownLatch}
import java.util.function.Supplier

import com.webank.eggroll.core.constant.SerdesTypes
import com.webank.eggroll.core.datastructure.TaskPlan
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta.{ErJob, ErPartition, ErStore, ErTask}
import com.webank.eggroll.core.util.ThreadPoolUtils

import scala.collection.mutable

case class EndpointCommand(commandURI: CommandURI, job: ErJob)

case class EndpointTaskCommand(commandURI: CommandURI, task: ErTask)


// todo: merge with command client
case class CollectiveCommand(taskPlan: TaskPlan) {
  def call(): Array[ErTask] = {
    val job = taskPlan.job
    val commandUri = taskPlan.uri

    val finishLatch = new CountDownLatch(job.inputs.size)
    val errors = new DistributedRuntimeException()
    val results = mutable.ArrayBuffer[ErTask]()

    val tasks = toTasks(job)

    tasks.par.map(task => {
      val completableFuture: CompletableFuture[ErTask] =
        CompletableFuture.supplyAsync(new CommandServiceSupplier(task, commandUri), CollectiveCommand.threadPool)
        .exceptionally(e => {
          errors.append(e)
          null
        })
        .whenCompleteAsync((result, exception) => {
          if (exception != null) {
            errors.append(exception)
          } else {
            results += result
          }
          finishLatch.countDown()
        })

      completableFuture.join()
    })

    finishLatch.await()

    if (!errors.check()) {
      errors.raise()
    }
    results.toArray
  }

  def toTasks(job: ErJob): Array[ErTask] = {
    val inputs: Array[ErStore] = job.inputs
    val inputPartitionSize = inputs.head.partitions.size

    val outputs: Array[ErStore] = job.outputs
    val result = mutable.ArrayBuffer[ErTask]()
    result.sizeHint(outputs(0).partitions.length)


    var aggregateOutputPartition: ErPartition = null
    if (outputs.head.partitions.size == 1) {
      aggregateOutputPartition = outputs.head.partitions.head
    }
    for (i <- 0 until inputPartitionSize) {
      val inputPartitions = mutable.ArrayBuffer[ErPartition]()
      val outputPartitions = mutable.ArrayBuffer[ErPartition]()

      inputs.foreach(input => inputPartitions.append(input.partitions(i)))

      if (aggregateOutputPartition != null) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        (inputs, outputs).zipped.foreach((input, output) => {
          val hasPartition = !output.partitions.isEmpty
          outputPartitions.append(
            if (hasPartition) output.partitions(i)
            else input.partitions(i).copy(storeLocator = output.storeLocator))
        })
      }

      result.append(ErTask(id = s"${job.id}-${i}", name = job.name, inputs = inputPartitions.toArray, outputs = outputPartitions.toArray, job = job))
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

}
