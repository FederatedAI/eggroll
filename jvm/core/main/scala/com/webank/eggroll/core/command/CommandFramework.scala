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
import com.webank.eggroll.core.clustermanager.session.SessionManager
import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, SerdesTypes, SessionConfKeys}
import com.webank.eggroll.core.datastructure.TaskPlan
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta.{ErJob, ErPartition, ErProcessor, ErSessionMeta, ErStore, ErStoreLocator, ErTask}
import com.webank.eggroll.core.util.ThreadPoolUtils
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

case class EndpointCommand(commandURI: CommandURI, job: ErJob)

case class EndpointTaskCommand(commandURI: CommandURI, task: ErTask)


// todo: merge with command client
case class CollectiveCommand(taskPlan: TaskPlan) {
  def call(): Array[ErTask] = {
    val job = taskPlan.job

    val commandUri = taskPlan.uri

    val finishLatch = new CountDownLatch(job.inputs.length)
    val errors = new DistributedRuntimeException()
    val results = mutable.ArrayBuffer[ErTask]()

    val tasks = toTasks(taskPlan)

    tasks.par.map(task => {
      val completableFuture: CompletableFuture[ErTask] =
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

  def toTasks(taskPlan: TaskPlan): Array[ErTask] = {
    val job = taskPlan.job
    val inputStores: Array[ErStore] = job.inputs
    val inputPartitionSize = inputStores.head.partitions.length
    val inputOptions = job.options
    val sessionId = inputOptions.get(SessionConfKeys.CONFKEY_SESSION_ID)
    if (StringUtils.isBlank(sessionId)) {
      throw new IllegalArgumentException("session id not exist")
    }

    val boundEggs = CollectiveCommand.getEggBound(inputStores.head)

    val outputStores: Array[ErStore] = job.outputs
    val result = mutable.ArrayBuffer[ErTask]()
    result.sizeHint(outputStores(0).partitions.length)

    var aggregateOutputPartition: ErPartition = null
    if (taskPlan.isAggregate) {
      aggregateOutputPartition = ErPartition(id = 0, storeLocator = outputStores.head.storeLocator, processor = boundEggs(0))
    }

    for (i <- 0 until inputPartitionSize) {
      val inputPartitions = mutable.ArrayBuffer[ErPartition]()
      val outputPartitions = mutable.ArrayBuffer[ErPartition]()

      inputStores.foreach(inputStore => {
        inputPartitions.append(
          ErPartition(id = i, storeLocator = inputStore.storeLocator, processor = boundEggs(i)))
      })

      if (taskPlan.isAggregate) {
        outputPartitions.append(aggregateOutputPartition)
      } else {
        outputStores.foreach(outputStore => {
          outputPartitions.append(
            ErPartition(id = i, storeLocator = outputStore.storeLocator, processor = boundEggs(i)))
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
  private val boundCache = mutable.Map[String, mutable.Map[String, Array[ErProcessor]]]()

  def getEggBound(store: ErStore): Array[ErProcessor] = {
    val options = store.options
    val sessionId = options.get(SessionConfKeys.CONFKEY_SESSION_ID)
    val bindingPlanId = options.get(SessionConfKeys.CONFKEY_SESSION_EGG_BINDING_ID)
    if (StringUtils.isAnyBlank(sessionId, bindingPlanId)) {
      throw new IllegalArgumentException(s"session id or bindingPlan id is blank for store ${store}. session id: ${sessionId}, binding plan id: ${bindingPlanId}")
    }

    if (!boundCache.contains(sessionId) || !boundCache(sessionId).contains(bindingPlanId)) {
      val clusterManagerClient = new ClusterManagerClient(
        options.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST, "localhost"),
        options.get(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, 4670).toInt)

      val boundEggProcessorBatch = clusterManagerClient.getBoundProcessorBatch(ErSessionMeta(id = sessionId, options = options))

      if (!boundCache.contains(sessionId)) {
        boundCache += (sessionId -> mutable.Map[String, Array[ErProcessor]]())
      }

      val sessionBound: mutable.Map[String,Array[ErProcessor]] = boundCache(sessionId)
      sessionBound += (bindingPlanId -> boundEggProcessorBatch.processors)
    }

    boundCache(sessionId)(bindingPlanId)
  }
}
