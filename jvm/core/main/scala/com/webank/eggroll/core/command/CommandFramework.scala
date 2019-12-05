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

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.{SerdesTypes, SessionConfKeys}
import com.webank.eggroll.core.datastructure.TaskPlan
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.util.{Logging, ThreadPoolUtils}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  def toTasks(taskPlan: TaskPlan): Array[ErTask] = {
    val job = taskPlan.job
    val inputStores: Array[ErStore] = job.inputs
    val inputPartitionSize = inputStores.head.storeLocator.totalPartitions
    val inputOptions = job.options
    val sessionId = inputOptions(SessionConfKeys.CONFKEY_SESSION_ID)
    if (StringUtils.isBlank(sessionId)) {
      throw new IllegalArgumentException("session id not exist")
    }

    CollectiveCommand.init(sessionId)

    val partitions = inputStores.head.partitions

    val outputStores: Array[ErStore] = job.outputs
    val result = mutable.ArrayBuffer[ErTask]()
    result.sizeHint(outputStores(0).partitions.length)

    var aggregateOutputPartition: ErPartition = null
    if (taskPlan.isAggregate) {
      aggregateOutputPartition = ErPartition(id = 0, storeLocator = outputStores.head.storeLocator, processor = CollectiveCommand.routeToEgg(partitions(0)))
    }

    for (i <- 0 until inputPartitionSize) {
      val inputPartitions = mutable.ArrayBuffer[ErPartition]()
      val outputPartitions = mutable.ArrayBuffer[ErPartition]()

      inputStores.foreach(inputStore => {
        inputPartitions.append(
          ErPartition(id = i, storeLocator = inputStore.storeLocator, processor = CollectiveCommand.routeToEgg(partitions(i))))
      })

      if (taskPlan.isAggregate) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        outputStores.foreach(outputStore => {
          outputPartitions.append(
            ErPartition(id = i, storeLocator = outputStore.storeLocator, processor = CollectiveCommand.routeToEgg(partitions(i))))
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
  private var sessionDeployment: ErServerSessionDeployment = _
  private var inited = false

  def init(sessionId: String): Unit = {
    if (inited) return
    // todo: add cm conf
    val sessionMeta = ErSessionMeta(id = sessionId)
    val clusterManagerClient = new ClusterManagerClient()
    val serverCluster = clusterManagerClient.getSessionServerNodes(sessionMeta)
    val rolls = clusterManagerClient.getSessionRolls(sessionMeta)
    val responseEggs: ErProcessorBatch = clusterManagerClient.getSessionEggs(sessionMeta)

    // todo:0: eliminate duplicate code in session manager register
    val eggs = mutable.Map[Long, ArrayBuffer[ErProcessor]]()

    responseEggs.processors.foreach(p => {
      eggs.get(p.serverNodeId) match {
        case Some(array) => array += p
        case None =>
          val arrayBuffer = ArrayBuffer[ErProcessor]()
          arrayBuffer += p
          eggs.put(p.serverNodeId, arrayBuffer)
      }
    })

    sessionDeployment = ErServerSessionDeployment(
      id = sessionId,
      serverCluster = serverCluster, rolls = rolls.processors,
      eggs = eggs.mapValues(v => v.toArray).toMap)

    inited = true
  }

  def routeToEgg(partition: ErPartition): ErProcessor = {
    val targetServerNode = partition.processor.serverNodeId
    val targetEggProcessors = sessionDeployment.eggs(targetServerNode).length
    val targetProcessor = (partition.id / targetEggProcessors) % targetEggProcessors

    sessionDeployment.eggs(targetServerNode)(targetProcessor)
  }

}
