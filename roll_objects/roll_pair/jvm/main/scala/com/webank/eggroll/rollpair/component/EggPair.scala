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

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErPartition, ErTask}
import com.webank.eggroll.core.serdes.DefaultScalaFunctorSerdes
import com.webank.eggroll.core.transfer.{GrpcTransferService, TransferClient}
import com.webank.eggroll.rollpair.io.RocksDBSortedKvAdapter

import scala.collection.mutable

class EggPair {
  def mapValues(task: ErTask): ErTask = {
    runTask(task)
  }

  def getDbPath(partition: ErPartition): String = {
    val storeLocator = partition.storeLocator
    val dbPathPrefix = "/tmp/eggroll/"
    dbPathPrefix + String.join(StringConstants.SLASH, storeLocator.storeType, storeLocator.namespace, storeLocator.name, partition.id)
  }

  def runTask(task: ErTask): ErTask = {
    val functors = task.job.functors

    val results = mutable.ListBuffer()
    var result = task

    if (task.name == "mapValues") {
      val f: Array[Byte] => Array[Byte] = EggPair.functorSerdes.deserialize(functors.head.body)
      val inputPartition = task.inputs.head
      val outputPartition = task.outputs.head

      val inputStore = new RocksDBSortedKvAdapter(getDbPath(inputPartition))
      val outputStore = new RocksDBSortedKvAdapter(getDbPath(outputPartition))

      outputStore.writeBatch(inputStore.iterate().map(t => (t._1, f(t._2))))

      inputStore.close()
      outputStore.close()
    } else if (task.name == "reduce") {
      val f: (Array[Byte], Array[Byte]) => Array[Byte] = EggPair.functorSerdes.deserialize(functors.head.body)

      val inputPartition = task.inputs.head
      val inputStore = new RocksDBSortedKvAdapter(getDbPath(inputPartition))
      var seqOpResult: Array[Byte] = null

      for (tmp <- inputStore.iterate()) {
        if (seqOpResult != null) {
          seqOpResult = f(seqOpResult, tmp._2)
        } else {
          seqOpResult = tmp._2
        }
      }

      // send seqOp result to "0"
      val partitionId = task.inputs.head.id
      val transferTag = task.job.name
      if ("0" == partitionId) {
        val partitionSize = task.job.inputs.head.partitions.size
        val queue = GrpcTransferService.getOrCreateQueue(transferTag, partitionSize).asInstanceOf[ArrayBlockingQueue[Array[Byte]]]

        var combOpResult = seqOpResult

        for (i <- 1 until partitionSize) {
          // todo: bind with configurations
          val seqOpResult = queue.poll(10, TimeUnit.MINUTES)

          combOpResult = f(combOpResult, seqOpResult)
        }

        val outputPartition = task.outputs.head
        val outputStore = new RocksDBSortedKvAdapter(getDbPath(outputPartition))

        outputStore.put("result".getBytes(), combOpResult)
        outputStore.close()
      } else {
        val transferClient = new TransferClient()

        transferClient.send(data = seqOpResult, tag = transferTag, serverNode = task.outputs.head.node)
      }
    }

    result
  }

  def reduce(task: ErTask): ErTask = {
    runTask(task)
  }
}

object EggPair {
  val functorSerdes = DefaultScalaFunctorSerdes()
}
