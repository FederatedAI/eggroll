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

package com.webank.eggroll.core.command

import java.util.concurrent.{CompletableFuture, CountDownLatch}
import java.util.function.Supplier

import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta.{ErJob, ErPartition, ErTask}
import com.webank.eggroll.core.util.ThreadPoolUtils

import scala.collection.mutable

case class EndpointCommand(commandURI: CommandURI, job: ErJob)

case class EndpointTaskCommand(commandURI: CommandURI, task: ErTask)

case class CollectiveCommand(commandURI: CommandURI, job: ErJob) {
  def call(): List[ErCommandResponse] = {
    val finishLatch = new CountDownLatch(job.inputs.size)
    val errors = new DistributedRuntimeException()
    val results = mutable.ListBuffer[ErCommandResponse]()

    val tasks = toTasks(job)

    tasks.par.map(task => {
      val completableFuture: CompletableFuture[ErCommandResponse] = CompletableFuture.supplyAsync(new CommandServiceSupplier(task, commandURI), CollectiveCommand.threadPool)
        .exceptionally(e => {
          errors.append(e)
          null
        })
        .thenApply(cr => {
          results += cr
          finishLatch.countDown()
          cr
        })

      completableFuture.join()
    })

    finishLatch.await()

    if (!errors.check()) {
      errors.raise()
    }
    results.toList
  }

  def toTasks(job: ErJob): List[ErTask] = {
    val result = mutable.ListBuffer[ErTask]()

    val inputs = job.inputs
    val outputs = job.outputs
    val inputPartitionSize = inputs.head.partitions.size

    var aggregateOutputPartition: ErPartition = null
    if (outputs.head.partitions.size == 1) {
      aggregateOutputPartition = outputs.head.partitions.head
    }
    for (i <- 0 until inputPartitionSize) {
      val inputPartitions = mutable.ListBuffer[ErPartition]()
      val outputPartitions = mutable.ListBuffer[ErPartition]()

      inputs.foreach(input => inputPartitions.append(input.partitions(i)))

      if (aggregateOutputPartition != null) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        outputs.foreach(output => outputPartitions.append(output.partitions(i)))
      }

      result.append(ErTask(id = job.id + "-" + i, name = job.name, inputs = inputPartitions.toList, outputs = outputPartitions.toList, job = job))
    }

    result.toList
  }
}

class CommandServiceSupplier(task: ErTask, command: CommandURI)
  extends Supplier[ErCommandResponse] {
  override def get(): ErCommandResponse = {
    val client = new CommandClient()
    client.send(task, command)
  }
}

object CollectiveCommand {
  val threadPool = ThreadPoolUtils.newFixedThreadPool(20, "command-")

}
