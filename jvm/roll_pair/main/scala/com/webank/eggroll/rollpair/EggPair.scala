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

package com.webank.eggroll.rollpair

import java.util.concurrent.TimeUnit

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.LinkedBlockingBroker
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta.{ErPartition, ErTask}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.transfer.{GrpcTransferClient, TransferService}
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.rollpair.io.RocksdbSortedKvAdapter

import scala.collection.mutable

class EggPair extends Logging {

  def runTask(task: ErTask): ErTask = {
    val functors = task.job.functors

    val results = mutable.ListBuffer()
    var result = task

    if (task.name == "mapValues") {
      val f: Array[Byte] => Array[Byte] = EggPair.functorSerdes.deserialize(functors.head.body)
      val inputPartition = task.inputs.head
      val outputPartition = task.outputs.head

      val inputAdapter = new RocksdbSortedKvAdapter(EggPair.getDbPath(inputPartition))
      val outputAdapter = new RocksdbSortedKvAdapter(EggPair.getDbPath(outputPartition))

      outputAdapter.writeBatch(inputAdapter.iterate().map(t => (t._1, f(t._2))))

      inputAdapter.close()
      outputAdapter.close()
    } else if (task.name == "map") {
      val f: (Array[Byte], Array[Byte]) => (Array[Byte], Array[Byte]) = EggPair.functorSerdes.deserialize(functors.head.body)
      val p: Array[Byte] => Int = EggPair.functorSerdes.deserialize(functors(1).body)

      val inputPartition = task.inputs.head
      val outputPartition = task.outputs.head
      val inputAdapter = new RocksdbSortedKvAdapter(EggPair.getDbPath(inputPartition))
      val outputStore = task.job.outputs.head

      // todo:2: encapsulates to shuffle command
      val shuffleBroker = new LinkedBlockingBroker[(Array[Byte], Array[Byte])]()
      val shuffler = new DefaultShuffler(task.job.id, shuffleBroker, outputStore, outputPartition, p)

      shuffler.start()
      inputAdapter.iterate().foreach(t => {
        shuffleBroker.put(f(t._1, t._2))
        // val result = f(t._1, t._2)
        // logInfo(s"partitionId: ${outputPartition.id}, result key: ${new String(result._1)}, last: ${result._1.last}")
      })
      shuffleBroker.signalWriteFinish()

      shuffler.errors.asInstanceOf[DistributedRuntimeException].raise()
      val shuffleFinished = shuffler.waitUntilFinished(10, TimeUnit.MINUTES)

      logInfo(s"total partitioned: ${shuffler.getTotalPartitionedCount()}")
    } else if (task.name == "reduce") {
      val f: (Array[Byte], Array[Byte]) => Array[Byte] = EggPair.functorSerdes.deserialize(functors.head.body)

      val inputPartition = task.inputs.head
      val inputStore = new RocksdbSortedKvAdapter(EggPair.getDbPath(inputPartition))
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
      if (0 == partitionId) {
        val partitionSize = task.job.inputs.head.partitions.size
        val queue = TransferService.getOrCreateBroker(transferTag, partitionSize)

        var combOpResult = seqOpResult

        for (i <- 1 until partitionSize) {
          // todo:2: bind with configurations
          val transferBatch = queue.poll(10, TimeUnit.MINUTES)
          val seqOpResult = transferBatch.getData.toByteArray

          combOpResult = f(combOpResult, seqOpResult)
        }

        val outputPartition = task.outputs.head
        val outputAdapter = new RocksdbSortedKvAdapter(EggPair.getDbPath(outputPartition))

        outputAdapter.put("result".getBytes(), combOpResult)
        outputAdapter.close()
      } else {
        val transferClient = new GrpcTransferClient()

        transferClient.sendSingle(data = seqOpResult, tag = transferTag, processor = task.outputs.head.processor)
      }

      inputStore.close()
    } else if (task.name == "join") {
      val f: (Array[Byte], Array[Byte]) => Array[Byte] = EggPair.functorSerdes.deserialize(functors.head.body)

      val leftPartition = task.inputs.head
      val rightPartition = task.inputs(1)
      val outputPartition = task.outputs.head

      val leftStore = new RocksdbSortedKvAdapter(EggPair.getDbPath(leftPartition))
      val rightStore = new RocksdbSortedKvAdapter(EggPair.getDbPath(rightPartition))
      val outputStore = new RocksdbSortedKvAdapter(EggPair.getDbPath(outputPartition))

      def doJoin(left: RocksdbSortedKvAdapter, right: RocksdbSortedKvAdapter): Iterator[(Array[Byte], Array[Byte])] = {
        val buffer = mutable.ListBuffer[(Array[Byte], Array[Byte])]()

        left.iterate().foreach(t => {
          val rightValueBytes = right.get(t._1)
          if (rightValueBytes != null) {
            buffer.append((t._1, f(t._2, rightValueBytes)))
          }
        })

        buffer.iterator
      }
      outputStore.writeBatch(doJoin(leftStore, rightStore))

      leftStore.close()
      rightStore.close()
      outputStore.close()
    }
    result
  }

  def map(task: ErTask): ErTask = {
    runTask(task)
  }

  def mapValues(task: ErTask): ErTask = {
    runTask(task)
  }

  def reduce(task: ErTask): ErTask = {
    runTask(task)
  }

  def join(task: ErTask): ErTask = {
    runTask(task)
  }
}

object EggPair {
  val functorSerdes = DefaultScalaSerdes()

  def getDbPath(partition: ErPartition): String = {
    val storeLocator = partition.storeLocator
    val dbPathPrefix = "/tmp/eggroll/"
    dbPathPrefix + String.join(StringConstants.SLASH, storeLocator.storeType, storeLocator.namespace, storeLocator.name, partition.id.toString)
  }
}
