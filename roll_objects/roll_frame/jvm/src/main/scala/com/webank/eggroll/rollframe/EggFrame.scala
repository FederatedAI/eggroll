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

import java.util.concurrent._

import com.webank.eggroll.core.io.util.IoUtils
import com.webank.eggroll.core.meta.{ErProcessor, ErTask}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.util.ThreadPoolUtils
import com.webank.eggroll.format.{FrameBatch, FrameDB, _}

import scala.collection.immutable.Range.Inclusive

class EggFrame {
  private val rootPath = "/tmp/unittests/RollFrameTests/file/"
  private val clusterManager = new ClusterManager
  private val serverNodes = clusterManager.getLiveProcessorBatch().processors
  private val transferService = new NioCollectiveTransfer(serverNodes)
  private val executorPool = ThreadPoolUtils.defaultThreadPool
  private val rootServer = serverNodes.head
  private val serdes = DefaultScalaSerdes()

  private def sliceByColumn(frameBatch: FrameBatch): List[(ErProcessor, Inclusive)] = {
    val columns = frameBatch.rootVectors.length
    val servers = serverNodes.length
    val partSize = (servers + columns - 1) / servers
    (0 until servers).map{ sid =>
      (serverNodes(sid), new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, columns), 1))
    }.toList
  }

  private def sliceByRow(parts: Int, frameBatch: FrameBatch): List[Inclusive] = {
    val rows = frameBatch.rowCount
    val partSize = (parts + rows - 1) / parts
    (0 until parts).map{ sid =>
      new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, rows), 1)
    }.toList
  }

  def runBroadcast(path: String): Unit = {
    val list = FrameDB.cache(path).readAll()
    serverNodes.foreach{server =>
      if(!server.equals(rootServer)) {
        list.foreach( fb => transferService.send(server.id, path, fb))
      }
    }
  }

  def runMapBatch(task: ErTask,
                  input: Iterator[FrameBatch],
                  output: FrameDB,
                  mapper: FrameBatch => FrameBatch): Unit = {
    // for concurrent writing
    val queuePath = task.id + "-doing"
    val queue = FrameDB.queue(queuePath, 1)
    input.foreach { store =>
      ThreadPoolUtils.defaultThreadPool.submit(new Runnable {
        override def run(): Unit = {
          queue.append(mapper(store))
        }
      })
    }
    output.writeAll(queue.readAll())
  }

  def runReduceBatch(task: ErTask,
                     input: Iterator[FrameBatch],
                     output:FrameDB,
                     reducer: (FrameBatch, FrameBatch) => FrameBatch,
                     byColumn: Boolean): Unit = {
    if(!input.hasNext) return
    val zero = input.next()
    runAggregateBatch(task, input, output, zero, reducer, (a, _)=> a, byColumn)
  }

  //TODO: allgather = List[(FB)=>FB]
  def runAggregateBatch(task: ErTask,
                        input: Iterator[FrameBatch],
                        output: FrameDB,
                        zeroValue: FrameBatch,
                        seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                        combOp: (FrameBatch, FrameBatch) => FrameBatch,
                        byColumn: Boolean): Unit = {
    val partition = task.inputs.head
    val batchSize = 1
    // TODO: more generally, like repartition?

    // TODO: route
    val localServer = clusterManager.getPreferredServer(store = task.job.inputs.head)
    println(s"runAggregateBatch ${localServer} ${partition.id}")
    var localQueue: FrameDB = null

    val zeroPath = "broadcast:" + task.job.id
    val zero: FrameBatch =
      if(zeroValue == null) {
        if(localServer.equals(rootServer))
          FrameDB.cache(zeroPath).readOne()
        else
          FrameDB.queue(zeroPath, 1).readOne()
      } else {
        zeroValue
      }
    // TODO: more generally, like repartition?
    if(batchSize == 1) {
      if(input.hasNext) {
        val fb = input.next()
        val parallel = Math.min(if (executorPool.getCorePoolSize > 0) executorPool.getCorePoolSize else 1, fb.rowCount) // split by row to grow parallel
        // for concurrent writing
        localQueue = FrameDB.queue(task.id + "-doing", parallel)
        sliceByRow(parallel, fb).foreach{case inclusive: Inclusive =>
          executorPool.submit(new Callable[Unit] {
            override def call(): Unit = {
              localQueue.append(seqOp(FrameUtils.copy(zero), fb.spliceByRow(inclusive.start, inclusive.end)))
            }
          })
        }
      }
    } else {
      val parallel = Math.min(executorPool.getCorePoolSize, batchSize) // reduce zero value copy
      // for concurrent writing
      localQueue = FrameDB.queue(task.id + "-doing", parallel)
      var batchIndex = 0
      (0 until parallel).foreach{ i =>
        if(input.hasNext) { // merge to avoid zero copy
          executorPool.submit(new Callable[Unit] {
            override def call(): Unit = {
              val tmpZero = FrameUtils.copy(zero)
              var tmp = seqOp(tmpZero, input.next())
              batchIndex += 1
              while (batchIndex < ((parallel + batchSize - 1) / batchSize) * i && input.hasNext) {
                tmp = seqOp(tmp, input.next())
                batchIndex += 1
              }
              localQueue.append(tmp)
            }
          })
        }
      }
    }

    if(localQueue == null) {
      return
    }

    // todo: local queue and result synchronization, maybe a countdown latch
    val resultIterator = localQueue.readAll()
    if(!resultIterator.hasNext) throw new IllegalStateException("empty result")
    var localBatch: FrameBatch = resultIterator.next()
    while (resultIterator.hasNext) {
      localBatch = combOp(localBatch, resultIterator.next())
    }
    val transferQueueSize = task.job.inputs.head.partitions.length - 1
    // TODO: check asynchronous call
    if(byColumn) {
      val splicedBatches = sliceByColumn(localBatch)
      // Don't block next receive step
      splicedBatches.foreach{ case (server, inclusive: Inclusive) =>
        val queuePath ="all2all:" + task.job.id + ":" + server.id
        if(!server.equals(localServer)) {
          transferService.send(server.id, queuePath, localBatch.sliceByColumn(inclusive.start, inclusive.end))
        }
      }
      splicedBatches.foreach{ case (server, inclusive: Inclusive) =>
        val queuePath ="all2all:" + task.job.id + ":" + server.id
        if(server.equals(localServer)){
          for(tmp <- FrameDB.queue(queuePath, transferQueueSize).readAll()) {
            localBatch = combOp(localBatch, tmp.spareByColumn(localBatch.rootVectors.length, inclusive.start, inclusive.end))
          }
        }
      }
    } else {
      val queuePath = "gather:" + task.job.id
      if(localServer.equals(rootServer)) {
        for(tmp <- FrameDB.queue(queuePath, transferQueueSize).readAll()) {
          localBatch = combOp(localBatch, tmp)
        }
      } else {
        transferService.send(rootServer.id, queuePath, localBatch)
      }
    }
    output.append(localBatch)
  }

  def runTask(task: ErTask): ErTask = {
    val inputPartition = task.inputs.head
    val outputPartition = task.outputs.head

    println(s"runTask, input: ${IoUtils.getPath(inputPartition)}, output: ${IoUtils.getPath(outputPartition)}")

    val inputDB = FrameDB(inputPartition)
    val outputDB = FrameDB(outputPartition)

    // todo: task status track
    val result = task
    val functors = task.job.functors

    task.name match {
      case EggFrame.broadcastTask =>
        runBroadcast(serdes.deserialize(functors.head.body))
      case EggFrame.mapBatchTask =>
        runMapBatch(task= task, input = inputDB.readAll(), output = outputDB, mapper = serdes.deserialize(task.job.functors.head.body))
      case EggFrame.reduceTask =>
        val reducer: (FrameBatch, FrameBatch) => FrameBatch = serdes.deserialize(functors.head.body)
        runReduceBatch(task, inputDB.readAll(), outputDB, reducer, byColumn = false)
      case EggFrame.aggregateBatchTask =>
        val zeroBytes = functors.head.body
        val zeroValue: FrameBatch = if(zeroBytes.isEmpty) null else FrameUtils.fromBytes(zeroBytes)
        val seqOp: (FrameBatch, FrameBatch) => FrameBatch = serdes.deserialize(functors(1).body)
        val combOp: (FrameBatch, FrameBatch) => FrameBatch = serdes.deserialize(functors(2).body)
        val byColumn: Boolean = serdes.deserialize(functors(3).body)
        runAggregateBatch(task, inputDB.readAll(), outputDB, zeroValue, seqOp, combOp, byColumn)
      case t => throw new UnsupportedOperationException(t)
    }
    outputDB.close()
    inputDB.close()
    result
  }
}

object EggFrame {
  val EggFrame = "EggFrame"
  val mapBatchTask = s"${EggFrame}.mapBatchTask"
  val reduceTask = s"${EggFrame}.reduceTask"
  val aggregateBatchTask = s"${EggFrame}.aggregateBatchTask"
  val broadcastTask = s"${EggFrame}.broadcastTask"
}
