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
 */

package com.webank.eggroll.rollframe

import java.util.concurrent._

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.constant.SessionConfKeys
import com.webank.eggroll.core.io.util.IoUtils
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErSessionMeta, ErTask}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.ThreadPoolUtils
import com.webank.eggroll.format.{FrameBatch, FrameStore, _}
import com.webank.eggroll.rollframe.pytorch.linalg.Matrices

import scala.collection.immutable.Range.Inclusive

class EggFrame {
  private val rootPath = "/tmp/unittests/RollFrameTests/file/"
  private lazy val serverNodes = EggFrame.session.processors
  private lazy val transferService = new NioCollectiveTransfer(serverNodes)
  private lazy val rootServer = serverNodes.head
  private val executorPool = ThreadPoolUtils.defaultThreadPool
  private val serdes = DefaultScalaSerdes()

  private def sliceByColumn(frameBatch: FrameBatch): List[(ErProcessor, Inclusive)] = {
    val columns = frameBatch.rootVectors.length
    val servers = serverNodes.length
    val partSize = (servers + columns - 1) / servers
    (0 until servers).map { sid =>
      (serverNodes(sid), new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, columns), 1))
    }.toList
  }

  private def sliceByRow(parts: Int, frameBatch: FrameBatch): List[Inclusive] = {
    val rows = frameBatch.rowCount
    val partSize = (parts + rows - 1) / parts
    (0 until parts).map { sid =>
      new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, rows), 1)
    }.toList
  }

  // TODO: remove roll site
  def runBroadcast(path: String): Unit = {
    val list = FrameStore.cache(path).readAll()
    serverNodes.foreach { server =>
      if (!server.equals(rootServer)) {
        list.foreach(fb => transferService.send(server.id, path, fb))
      }
    }
  }

  def runMapBatch(task: ErTask,
                  input: Iterator[FrameBatch],
                  output: FrameStore,
                  mapper: FrameBatch => FrameBatch): Unit = {
    // for concurrent writing
    val queuePath = task.id + "-doing"
    // total mean batch size, if given more than one, it just get one.
    val queue = FrameStore.queue(queuePath, 1)
    input.foreach { fb =>
      ThreadPoolUtils.defaultThreadPool.submit(new Runnable {
        override def run(): Unit = {
          queue.append(mapper(fb))
        }
      })
    }
    output.writeAll(queue.readAll())
  }

  def runMatMulV1(task: ErTask,
                  input: Iterator[FrameBatch],
                  output: FrameStore,
                  matrix: Array[Double],
                  rows: Int,
                  cols: Int
               ): Unit = {
    val queuePath = task.id + "-doing"
    // total mean batch size, if given more than one, it just get one.
    val queue = FrameStore.queue(queuePath, 1)
    input.foreach { fb =>
      ThreadPoolUtils.defaultThreadPool.submit(new Runnable {
        override def run(): Unit = {
          println(fb.rowCount,fb.fieldCount)
          var start = System.currentTimeMillis()
          val cb = fb.toColumnVectors
          println(s"FrameBatch to ColumnVectors time = ${System.currentTimeMillis() - start}")
          start = System.currentTimeMillis()
          val resFb = Matrices.matMulToFbV1(cb, matrix, rows, cols)
          println(s"matMul and store as Fb time = ${System.currentTimeMillis() - start}")
          queue.append(resFb)
        }
      })
    }
    output.writeAll(queue.readAll())
  }

  def runMatMul(task: ErTask,
                input: Iterator[FrameBatch],
                output: FrameStore,
                matrix: Array[Double],
                rows: Int,
                cols: Int
               ): Unit = {
    val queuePath = task.id + "-doing"
    // total mean batch size, if given more than one, it just get one.
    val queue = FrameStore.queue(queuePath, 1)
    input.foreach { fb =>
      ThreadPoolUtils.defaultThreadPool.submit(new Runnable {
        override def run(): Unit = {
          var start = System.currentTimeMillis()
          val cf = new ColumnFrame(fb,rows)
          println(s"get CF time = ${System.currentTimeMillis() - start}")
          start = System.currentTimeMillis()
          val resFb = Matrices.matMulToFb(cf, matrix, rows, cols)
          println(s"matMul and store as Fb time = ${System.currentTimeMillis() - start}")
          queue.append(resFb)
        }
      })
    }
    output.writeAll(queue.readAll())
  }

  def runReduceBatch(task: ErTask,
                     input: Iterator[FrameBatch],
                     output: FrameStore,
                     reducer: (FrameBatch, FrameBatch) => FrameBatch,
                     byColumn: Boolean): Unit = {
    if (!input.hasNext) return
    val zero = input.next()
    runAggregateBatch(task, input, output, zero, (a, _) => a, reducer, byColumn, -1)
  }

  //TODO: allgather = List[(FB)=>FB]
  def runAggregateBatch(task: ErTask,
                        input: Iterator[FrameBatch],
                        output: FrameStore,
                        zeroValue: FrameBatch,
                        seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                        combOp: (FrameBatch, FrameBatch) => FrameBatch,
                        byColumn: Boolean,
                        threadsNum: Int): Unit = {
    val partition = task.inputs.head
    val batchSize = 1
    // TODO: more generally, like repartition?
    // TODO: route
    //    val localServer = clusterManager.getPreferredServer(store = task.job.inputs.head)(partition.id)
    val localServer = partition.processor
    println(s"runAggregateBatch: partion.id = ${partition.id}")
    var localQueue: FrameStore = null

    // TODO: don't finish broadcast
    val zeroPath = "broadcast:" + task.job.id
    val zero: FrameBatch =
      if (zeroValue == null) {
        if (localServer.equals(rootServer))
          FrameStore.cache(zeroPath).readOne()
        else
          FrameStore.queue(zeroPath, 1).readOne()
      } else {
        zeroValue
      }
    // TODO: more generally, like repartition?
    if (batchSize == 1) {
      if (input.hasNext) {
        val fb = input.next()
        // use muti-thread by rows ,for example,parallel = 2, 100 rows can split to [0,50] and [50,100]
        // for concurrent writing
        // TODO: specify thread num, if zero value is to large , copy too many zero value will cause OOM
        // TODO: whether care about memory state and then decide thread num.
        val parallel: Int = if (threadsNum < 0) {
          val availableProcessors = Runtime.getRuntime.availableProcessors() / 2 // testing suggestion
          val eachThreadCount = 1000
          val tmpParallel = fb.rowCount / eachThreadCount + 1
          if (tmpParallel < availableProcessors) tmpParallel else availableProcessors
          //          2 * availableProcessors
        } else {
          threadsNum
        }
        localQueue = FrameStore.queue(task.id + "-doing", parallel)

        println(s"egg parallel = $parallel")
        sliceByRow(parallel, fb).foreach { case inclusive: Inclusive =>
          executorPool.submit(new Callable[Unit] {
            override def call(): Unit = {
              var start = System.currentTimeMillis()
              val tmpZeroValue = FrameUtils.fork(zero)
              println(s"fork time: ${System.currentTimeMillis() - start} ms")
              start = System.currentTimeMillis()
              localQueue.append(seqOp(tmpZeroValue, fb.sliceByRow(inclusive.start, inclusive.end)))
              println(s"seqOp time: ${System.currentTimeMillis() - start}")
            }
          })
        }
      } else {
        // for reduce op
        localQueue = FrameStore.queue(task.id + "-doing", 1)
        localQueue.append(zero)
      }
    } else {
      // TODO: unfinished
      val parallel = Math.min(executorPool.getCorePoolSize, batchSize) // reduce zero value copy
      // for concurrent writing
      localQueue = FrameStore.queue(task.id + "-doing", parallel)
      var batchIndex = 0
      (0 until parallel).foreach { i =>
        if (input.hasNext) { // merge to avoid zero copy
          executorPool.submit(new Callable[Unit] {
            override def call(): Unit = {
              val tmpZero = FrameUtils.fork(zero)
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

    if (localQueue == null) {
      return
    }

    // todo: local queue and result synchronization, maybe a countdown latch
    val resultIterator = localQueue.readAll()
    if (!resultIterator.hasNext) throw new IllegalStateException("empty result")
    var localBatch: FrameBatch = resultIterator.next()
    while (resultIterator.hasNext) {
      localBatch = combOp(localBatch, resultIterator.next())
    }
    val transferQueueSize = task.job.inputs.head.storeLocator.totalPartitions - 1
    require(transferQueueSize > 0, s"transferQueueSize:${transferQueueSize}, task:${task}")
    // TODO: check asynchronous call
    if (byColumn) {
      val splicedBatches = sliceByColumn(localBatch)
      // Don't block next receive step
      splicedBatches.foreach { case (server, inclusive: Inclusive) =>
        val queuePath = "all2all:" + task.job.id + ":" + server.id
        if (!server.equals(localServer)) {
          transferService.send(server.id, queuePath, localBatch.sliceByColumn(inclusive.start, inclusive.end))
        }
      }

      splicedBatches.foreach { case (server, inclusive: Inclusive) =>
        val queuePath = "all2all:" + task.job.id + ":" + server.id
        if (server.equals(localServer)) {
          for (tmp <- FrameStore.queue(queuePath, transferQueueSize).readAll()) {
            localBatch = combOp(localBatch, tmp.spareByColumn(localBatch.rootVectors.length, inclusive.start, inclusive.end))
          }
        }
      }
    } else {
      val queuePath = "gather:" + task.job.id
      if (localServer.commandEndpoint.host.equals(rootServer.commandEndpoint.host)) {
        // the same root server
        if (partition.id == 0) {
          for (tmp <- FrameStore.queue(queuePath, transferQueueSize).readAll()) {
            localBatch = combOp(localBatch, tmp)
          }
          output.append(localBatch)
        } else {
          // the same root server but different partition
          FrameStore.queue(queuePath, -1).writeAll(Iterator(localBatch))
        }
      } else {
        transferService.send(rootServer.id, queuePath, localBatch)
      }
    }
  }

  def runTask(task: ErTask): ErTask = {
    val inputPartition = task.inputs.head
    val outputPartition = task.outputs.head

    println(s"run ${task.job.name}, input: ${IoUtils.getPath(inputPartition)}, output: ${IoUtils.getPath(outputPartition)}")

    val inputDB = FrameStore(inputPartition)
    val outputDB = FrameStore(outputPartition)

    // todo: task status track
    val result = task
    val functors = task.job.functors

    task.name match {
      case EggFrame.broadcastTask =>
        runBroadcast(serdes.deserialize(functors.head.body))
      case EggFrame.mapBatchTask =>
        runMapBatch(task = task, input = inputDB.readAll(), output = outputDB, mapper = serdes.deserialize(task.job.functors.head.body))
      case EggFrame.reduceTask =>
        val reducer: (FrameBatch, FrameBatch) => FrameBatch = serdes.deserialize(functors.head.body)
        runReduceBatch(task, inputDB.readAll(), outputDB, reducer, byColumn = false)
      case EggFrame.aggregateBatchTask =>
        val zeroBytes = functors.head.body
        val zeroValue: FrameBatch = if (zeroBytes.isEmpty) null else FrameUtils.fromBytes(zeroBytes)
        val seqOp: (FrameBatch, FrameBatch) => FrameBatch = serdes.deserialize(functors(1).body)
        val combOp: (FrameBatch, FrameBatch) => FrameBatch = serdes.deserialize(functors(2).body)
        val byColumn: Boolean = serdes.deserialize(functors(3).body)
        val threadsNum: Int = serdes.deserialize(functors(5).body)
        runAggregateBatch(task, inputDB.readAll(), outputDB, zeroValue, seqOp, combOp, byColumn, threadsNum)
      case EggFrame.mulMulTask =>
        val matrix:Array[Double] = serdes.deserialize(functors.head.body)
        val rows: Int = serdes.deserialize(functors(1).body)
        val cols: Int = serdes.deserialize(functors(2).body)
        runMatMul(task, inputDB.readAll(), outputDB, matrix, rows, cols)
      case EggFrame.mulMulTaskV1 =>
        val matrix:Array[Double] = serdes.deserialize(functors.head.body)
        val rows: Int = serdes.deserialize(functors(1).body)
        val cols: Int = serdes.deserialize(functors(2).body)
        runMatMulV1(task, inputDB.readAll(), outputDB, matrix, rows, cols)
      case t => throw new UnsupportedOperationException(t)
    }
    outputDB.close()
    inputDB.close()
    result
  }
}

object EggFrame {
  private lazy val session =  new ClusterManagerClient().getSession(ErSessionMeta(
    id = StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_ID, null)))

  val EggFrame = "EggFrame"
  val mapBatchTask = s"${EggFrame}.mapBatchTask"
  val reduceTask = s"${EggFrame}.reduceTask"
  val aggregateBatchTask = s"${EggFrame}.aggregateBatchTask"
  val broadcastTask = s"${EggFrame}.broadcastTask"
  val mulMulTask = "mulMulTask"
  val mulMulTaskV1 = "mulMulTaskV1"

  def getDoubleSchema(fieldCount: Int): String = {
    val sb = new StringBuilder
    sb.append(
      """{
                 "fields": [""")
    (0 until fieldCount).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }
}
