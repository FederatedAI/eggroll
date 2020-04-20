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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.format.{FrameBatch, _}
import com.webank.eggroll.rollframe.pytorch.{Matrices, Script}

import scala.collection.immutable.Range.Inclusive

// TODO: care about client task grpc whether closed and thread pool whether closed
// TODO: always close in finally

class RollFrameContext private[eggroll](val session: ErSession) {
  lazy val serverNodes: Array[ErProcessor] = session.processors
  lazy val rollNodes: ErProcessor = session.processors(0)

  // TODO: set private
  lazy val frameTransfer: NioFrameTransfer = new NioFrameTransfer(serverNodes)

  val defaultStoreType: String = StringConstants.FILE

  def dumpCache(store: ErStore): ErStore = {
    val cacheStore = forkStore(store, store.storeLocator.namespace, store.storeLocator.name, StringConstants.CACHE)
    load(store).mapBatch(f => f, cacheStore)
    cacheStore
  }

  def load(store: ErStore): RollFrame = RollFrame(store, this)

  def load(namespace: String, name: String, options: Map[String, String] = Map()): RollFrame = {
    // TODO:1: use snake case universally?
    val storeType = options.getOrElse("store_type", defaultStoreType)
    val totalPartitions = options.getOrElse("total_partitions", "1").toInt
    val store = ErStore(storeLocator = ErStoreLocator(
      namespace = namespace,
      name = name,
      storeType = storeType,
      totalPartitions = totalPartitions
    ))
    val loaded = session.clusterManagerClient.getOrCreateStore(store)
    new RollFrame(loaded, this)
  }

  def stopSession(): Unit = {
    session.clusterManagerClient.stopSession(session.sessionMeta)
  }

  /**
   * Provides two way to create/update or get Store by totalPartitions.
   * IF totalPartitions = 0, return store with partitions the same as processors.
   * IF totalPartitions = N, return Store with N partition and uniformly distributed on each process,
   * For example: there are 3 processor and totalPartitions is 5, store would has [0,1][2,3],[4] partitions.
   *
   * @param namespace       Store namespace
   * @param name            store name
   * @param storeType       store type
   * @param totalPartitions totalPartitions,{-1,N},
   * @return ErStore
   */
  def createStore(namespace: String, name: String, storeType: String = defaultStoreType, totalPartitions: Int = 0,
                  keep: Boolean = false): ErStore = {
    val storeLocator: ErStoreLocator = ErStoreLocator(
      namespace = namespace,
      name = name,
      storeType = storeType,
      totalPartitions = if (totalPartitions == 0) serverNodes.length else totalPartitions
    )
    val partitions: Array[ErPartition] = totalPartitions match {
      case 0 =>
        // partitions num equal processors
        serverNodes.indices.map(i => ErPartition(id = i, storeLocator = storeLocator, processor = serverNodes(i))).toArray
      case _ =>
        val _processors = assignProcessors(serverNodes, totalPartitions)
        (0 until totalPartitions).map { i =>
          ErPartition(id = i, storeLocator = storeLocator, processor = _processors(i))
        }.toArray
    }
    val store = ErStore(storeLocator = storeLocator, partitions = partitions)
    if (keep) {
      session.clusterManagerClient.deleteStore(storeLocator)
      session.clusterManagerClient.getOrCreateStore(store)
    }
    store
  }

  def forkStore(oldStore: ErStore, namespace: String, name: String, storeType: String = defaultStoreType,
                keep: Boolean = false): ErStore = {
    val storeLocator: ErStoreLocator = oldStore.storeLocator.copy(namespace = namespace, name = name, storeType = storeType,
      totalPartitions = oldStore.storeLocator.totalPartitions)
    val partitions: Array[ErPartition] = oldStore.partitions.map(i => i.copy(storeLocator = storeLocator))
    val store = ErStore(storeLocator = storeLocator, partitions = partitions)
    if (keep) {
      session.clusterManagerClient.deleteStore(storeLocator)
      session.clusterManagerClient.getOrCreateStore(store)
    }
    store
  }

  def getStore(namespace: String, name: String, storeType: String = defaultStoreType): ErStore = {
    val storeLocator = ErStoreLocator(
      namespace = namespace,
      name = name,
      storeType = storeType
    )
    session.clusterManagerClient.getStore(storeLocator)
  }

  private def assignProcessors(processors: Array[ErProcessor], totalPartitions: Int): Array[ErProcessor] = {
    val nodesLength = processors.length
    val quotient = totalPartitions / nodesLength
    val remainder = totalPartitions % nodesLength
    val processorsCounts = Array.fill(nodesLength)(quotient)
    (0 until remainder).foreach(i => processorsCounts(i) += 1)
    var res = Array[ErProcessor]()
    (0 until nodesLength).foreach(i => res = res ++ Array.fill(processorsCounts(i))(processors(i)))
    res
  }

  def broadcast(path: String, frameBatches: Iterator[FrameBatch]): Unit = {
    frameTransfer.broadcast(path, frameBatches)
  }

  def broadcast(path: String, frameBatch: FrameBatch): Unit = broadcast(path, Iterator(frameBatch))
}

object RollFrameContext {
  //  StaticErConf.addProperties("conf/eggroll.properties")

  def apply(session: ErSession): RollFrameContext = new RollFrameContext(session)

  def apply(): RollFrameContext = {
    val opts = Map("processor_types" -> "egg_frame", "processor_plan.egg_frame" -> "uniform")
    apply(new ErSession(options = opts))
  }
}

// create a instance when start a new job
// TODO: reuse ErJob generate and separate client mode and cluster mode
class RollFrame private[eggroll](val store: ErStore, val ctx: RollFrameContext) extends Logging {
  val serdes = new DefaultScalaSerdes
  val rfScheduler = new RollFrameScheduler(ctx.session)

  private val seqJobId = new AtomicInteger()
  private val jobIdDf = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")

  def genJobId(): String = {
    jobIdDf.format(new Date()) + "_" + seqJobId.incrementAndGet()
  }

  def torchMap(path: String, parameters: Array[Double], output: ErStore = null): RollFrame = {
    val func: (FrameBatch) => FrameBatch = { fb =>
      Script.runTorchMap(path, fb, parameters)
    }

    mapBatch(func, output)
  }

  def torchMerge(path: String, parameters: Array[Double] = Array(), output: ErStore = null): RollFrame = {
    val partitionNums = store.partitions.length

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        val queuePath = "gather:" + task.job.id
        val partition = task.inputs.head
        val localServer = partition.processor
        val localBatch = input.next()
        if (localServer.commandEndpoint.host.equals(ctx.rootServer.commandEndpoint.host)) {
          // the same root server
          if (partition.id == 0) {
            println("run merge ....")
            FrameStore.queue(queuePath, -1).writeAll(Iterator(localBatch))
            val allFrameBatch = FrameStore.queue(queuePath, partitionNums).readAll()
            val resFb = Script.runTorchMerge(path, allFrameBatch, parameters)
            output.append(resFb)
          } else {
            // the same root server but different partition
            FrameStore.queue(queuePath, -1).writeAll(Iterator(localBatch))
          }
        } else {
          ctx.frameTransfer.send(ctx.rootServer.id, queuePath, localBatch)
        }
        null
    }

    runUnaryJob("torchMerge", func, output = output)
  }

  @deprecated
  def matMulV1(m: Array[Double], rows: Int, cols: Int, output: ErStore = null): RollFrame = {
    val func: (FrameBatch) => FrameBatch = { fb =>
      println(fb.rowCount, fb.fieldCount)
      var start = System.currentTimeMillis()
      val cb = fb.toColumnVectors
      println(s"FrameBatch to ColumnVectors time = ${System.currentTimeMillis() - start}")
      start = System.currentTimeMillis()
      val resFb = Matrices.matMulToFbV1(cb, m, rows, cols)
      println(s"matMul and store as Fb time = ${System.currentTimeMillis() - start}")
      resFb
    }

    mapBatch(func, output)
  }

  @deprecated
  def matMul(m: Array[Double], rows: Int, cols: Int, output: ErStore = null): RollFrame = {
    val func: (FrameBatch) => FrameBatch = { fb =>
      println(fb.rowCount, fb.fieldCount)
      var start = System.currentTimeMillis()
      val cb = fb.toColumnVectors
      println(s"FrameBatch to ColumnVectors time = ${System.currentTimeMillis() - start}")
      start = System.currentTimeMillis()
      val resFb = Matrices.matMulToFbV1(cb, m, rows, cols)
      println(s"matMul and store as Fb time = ${System.currentTimeMillis() - start}")
      resFb
    }

    mapBatch(func, output)
  }

  private def runUnaryJob(jobType: String,
                          func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair, jobId: String = null,
                          output: ErStore = null): RollFrame = {
    val retFunc: (EggFrameContext, ErTask) => ErPair = { (ctx, task) =>
      val inputPartition = task.inputs.head
      val outputPartition = task.outputs.head
      ctx.logInfo(s"""start runUnary ${task.job.name}, input: $inputPartition, output: $outputPartition""")
      val inputDB = FrameStore(inputPartition)
      val outputDB = FrameStore(outputPartition)
      val ret = func(ctx, task, inputDB.readAll(), outputDB)
      ctx.logInfo(s"""finish runUnary ${task.job.name}, input: $inputPartition, output: $outputPartition""")
      if (ret == null) {
        ErPair(key = ctx.serdes.serialize(inputPartition.id), value = Array())
      } else {
        ret
      }
    }
    val job = ErJob(id = if (jobId == null) jobType + "_" + genJobId() else jobId,
      name = jobType,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobId) else output),
      functors = Array(ErFunctor(name = RollFrame.mapBatch, body = serdes.serialize(retFunc))))
    rfScheduler.run(job)
    ctx.load(job.outputs.head)
  }

  def mapBatch(f: FrameBatch => FrameBatch, output: ErStore = null): RollFrame = {
    val jobType = "mapBatch"
    val jobId = jobType + "_" + genJobId()

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        //         for concurrent writing
        val queuePath = task.id + "-doing"
        //         total mean batch size, if given more than one, it just get one.
        val queue = FrameStore.queue(queuePath, 1)
        input.foreach { fb =>
          ctx.executorPool.submit(new Runnable {
            override def run(): Unit = {
              val start = System.currentTimeMillis()
              queue.append(f(fb))
              println(s"thread: ${Thread.currentThread().getName},time of ${System.currentTimeMillis() - start} ms")
            }
          })
        }
        println(s"thread: ${Thread.currentThread().getName}")
        output.writeAll(queue.readAll())
        output.close()
        null
    }

    runUnaryJob("mapBatch", func, jobId = jobId, output = output)
  }


  // TODO: add reduce by rows operation
  /**
   * reduce frameBatchs between different partitions
   * eg:
   * 1 1 1   2 2 2   3 3 3
   * 1 1 1 + 2 2 2 = 3 3 3
   * 1 1 1   2 2 2   3 3 3
   *
   * @param f      reducer
   * @param output ErStore
   * @return
   */
  def reduce(f: (FrameBatch, FrameBatch) => FrameBatch, output: ErStore = null): RollFrame = {
    aggregate(null, f, f, output = output)
  }

  def aggregate(zeroValue: FrameBatch,
                seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                combOp: (FrameBatch, FrameBatch) => FrameBatch,
                byColumn: Boolean = false,
                broadcastZeroValue: Boolean = false,
                threadsNum: Int = -1,
                output: ErStore = null): RollFrame = {
    val jobType = "aggregate"
    val jobId = jobType + "_" + genJobId()
    val zeroValueBytes = if (broadcastZeroValue) {
      ctx.broadcast("broadcast:" + jobId, zeroValue)
      Array[Byte]()
    } else if (zeroValue != null) {
      FrameUtils.toBytes(zeroValue)
    } else {
      Array[Byte]()
    }

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>

        val zeroValue: FrameBatch = if (zeroValueBytes.isEmpty) null else FrameUtils.fromBytes(zeroValueBytes)
        val partition = task.inputs.head
        val batchSize = 1
        // TODO: more generally, like repartition?
        // TODO: route
        //    val localServer = clusterManager.getPreferredServer(store = task.job.inputs.head)(partition.id)
        val localServer = partition.processor
        ctx.logInfo(s"runAggregateBatch: jobId=${task.job.id}, partitionId=${partition.id}, root=${ctx.rootServer}")
        var localQueue: FrameStore = null

        // TODO: don't finish broadcast
        val zeroPath = "broadcast:" + task.job.id
        val zero: FrameBatch =
          if (zeroValue == null) {
            if (broadcastZeroValue) {
              FrameStore.cache(zeroPath).readOne()
            } else {
              // reduce need't zero value
              if (input.hasNext) {
                input.next()
              } else {
                null
              }
            }
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
            ctx.sliceByRow(parallel, fb).foreach { inclusive: Inclusive =>
              ctx.executorPool.submit(new Callable[Unit] {
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
          val parallel = Math.min(ctx.executorPool.getCorePoolSize, batchSize) // reduce zero value copy
          // for concurrent writing
          localQueue = FrameStore.queue(task.id + "-doing", parallel)
          var batchIndex = 0
          (0 until parallel).foreach { i =>
            if (input.hasNext) { // merge to avoid zero copy
              ctx.executorPool.submit(new Callable[Unit] {
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

        //      if (localQueue == null) {
        //        return null
        //      }
        // caution: cannot use return, in val func: xx,
        // and cannot use def func(xx) in scala 2.11 because of serder problem
        if (localQueue != null) {
          // todo: local queue and result synchronization, maybe a countdown latch
          val resultIterator = localQueue.readAll()
          if (!resultIterator.hasNext) throw new IllegalStateException("empty result")
          var localBatch: FrameBatch = resultIterator.next()
          while (resultIterator.hasNext) {
            localBatch = combOp(localBatch, resultIterator.next())
          }
          val transferQueueSize = task.job.inputs.head.storeLocator.totalPartitions - 1
          require(transferQueueSize > -1, s"""transferQueueSize:$transferQueueSize, task:$task""")
          // TODO: check asynchronous call

          if (byColumn) {
            val slicedBatches = ctx.sliceByColumn(localBatch)
            // Don't block next receive step
            slicedBatches.foreach { case (server, inclusive: Inclusive) =>
              val queuePath = "all2all:" + task.job.id + ":" + server.id
              if (!server.equals(localServer)) {
                ctx.frameTransfer.send(server.id, queuePath, localBatch.sliceByColumn(inclusive.start, inclusive.end))
              }
            }

            slicedBatches.foreach { case (server, inclusive: Inclusive) =>
              val queuePath = "all2all:" + task.job.id + ":" + server.id
              if (server.equals(localServer)) {
                for (tmp <- FrameStore.queue(queuePath, transferQueueSize).readAll()) {
                  localBatch = combOp(localBatch, tmp.spareByColumn(localBatch.rootVectors.length, inclusive.start, inclusive.end))
                }
              }
            }
          } else {
            val queuePath = "gather:" + task.job.id
            if (localServer.commandEndpoint.host.equals(ctx.rootServer.commandEndpoint.host)) {
              // the same root server
              if (partition.id == 0) {
                println(s"transferQueueSize = ${transferQueueSize}")
                // parallel combine
                FrameStore.queue(queuePath, -1).writeAll(Iterator(localBatch))
                val latch = new CountDownLatch(transferQueueSize)
                (0 until transferQueueSize).foreach { i =>
                  val iter = FrameStore.queue(queuePath, 2).readAll()
                  val a = iter.next()
                  val b = iter.next()
                  ctx.executorPool.submit(new Callable[Unit] {
                    override def call(): Unit = {
                      FrameStore.queue(queuePath, -1).writeAll(Iterator(combOp(a, b)))
                      latch.countDown()
                    }
                  })
                }
                latch.await(10, TimeUnit.SECONDS)
                output.writeAll(FrameStore.queue(queuePath, 1).readAll())
                //   serial combine
                //      for (tmp <- FrameStore.queue(queuePath, transferQueueSize).readAll()) {
                //            localBatch = combOp(localBatch, tmp)
                //       }
                //       output.append(localBatch)
              } else {
                // the same root server but different partition
                FrameStore.queue(queuePath, -1).writeAll(Iterator(localBatch))
              }
            } else {
              // TODO: combine in every processor, reduce time of network transfer
              // - use different clients
              // val ft = new NioFrameTransfer(ctx.serverNodes)
              // ft.send(ctx.rootServer.id, queuePath, localBatch)
              // - use the same client
              ctx.frameTransfer.synchronized(ctx.frameTransfer.send(ctx.rootServer.id, queuePath, localBatch))
            }
          }
          output.close()
        }
        null
    }

    runUnaryJob("aggregate", func, jobId = jobId, output = output)
  }
}

object RollFrame {
  val rollFrame = "RollFrame"
  val eggFrame = "EggFrame"
  val mapBatch = "mapBatch"
  val reduce = "reduce"
  val aggregate = "aggregate"
  val broadcast = "broadcast"
  val mulMul = "mulMulTask"

  def apply(store: ErStore, ctx: RollFrameContext): RollFrame = new RollFrame(store, ctx)
}
