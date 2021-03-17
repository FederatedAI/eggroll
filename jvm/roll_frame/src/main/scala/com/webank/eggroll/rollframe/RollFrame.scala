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

import java.nio.{ByteOrder, DoubleBuffer}
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.{Callable, CountDownLatch, Future}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Range.Inclusive
import scala.util.Random
import jep.{DirectNDArray, NDArray}
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.{SessionStatus, StringConstants}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.util.TimeUtils
import com.webank.eggroll.format._
import com.webank.eggroll.rollframe.embpython.{LocalThreadPythonInterp, PyInterpreter}
import com.webank.eggroll.rollframe.pytorch.{Matrices, Script}
import com.webank.eggroll.util.{Logging, SchemaUtil}
import io.netty.util.internal.PlatformDependent

// TODO: care about client task grpc whether closed and thread pool whether closed
// TODO: always close in finally

class RollFrameContext private[eggroll](val session: ErSession) extends Logging {
  lazy val eggProcessors: Array[ErProcessor] = session.processors
  lazy val rollNodes: ErProcessor = eggProcessors.head
  // get first processor from each serverNodes' processors
  lazy val serverNodes: Array[ErProcessor] = {
    var list = Array[ErProcessor]()
    var tmpServerId: Long = -1
    session.processors.foreach { p =>
      if (tmpServerId != p.serverNodeId) {
        list = list :+ p
        tmpServerId = p.serverNodeId
      }
    }
    list
  }


  // TODO: set private
  lazy val frameTransfer: NioFrameTransfer = new NioFrameTransfer(eggProcessors)

  val defaultStoreType: String = StringConstants.FILE


  /**
   * Create new ErStore with cache type by using map op.
   *
   * @param store input ErStore with other store type, such as file.
   * @return output ErStore with cache type.
   */
  def dumpCache(store: ErStore): ErStore = {
    val cacheStore = forkStore(store, store.storeLocator.namespace, store.storeLocator.name, StringConstants.CACHE)
    load(store).mapBatch(f => f, cacheStore)
    cacheStore
  }


  /**
   * Vertical stack all FrameBatch in each server node
   *
   * @param store : input ErStore
   * @return output ErStore haven stacked
   */
  def combineDoubleFbs(store: ErStore, singleThread: Boolean = true): ErStore = {
    val cacheStore = if (singleThread) {
      // todo:add parameters check
      val partitions = if (store.partitions.length < serverNodes.length) {
        store.partitions.length
      } else {
        serverNodes.length
      }
      createStore(store.storeLocator.namespace, "all_" + store.storeLocator.name, StringConstants.CACHE, partitions)
    } else {
      val partitions = if (store.partitions.length < eggProcessors.length) {
        store.partitions.length
      } else {
        eggProcessors.length
      }
      createStore(store.storeLocator.namespace, "all_" + store.storeLocator.name, StringConstants.CACHE, partitions, singleThread = false)
    }

    // repartition
    val dir = FrameStore.getStoreDir(store)
    load(cacheStore).genData(_ => {
      // get all FrameBatch
      val elements = JvmFrameStore.getJvmFrameBatches(dir)
      val totalRows = elements.foldLeft(0)((a, b) => {
        a + b._2.head.rowCount
      })
      val fbs = new FrameBatch(new FrameSchema(elements.head._2.head.rootSchema.arrowSchema.getSchema), totalRows)
      (0 until fbs.fieldCount).foreach { f =>
        var rowBatch = 0
        elements.foreach { i =>
          // serial
          // todo: use copyMemory
          val fb = i._2.head
          (0 until fb.rowCount).foreach { r =>
            fbs.writeDouble(f, rowBatch + r, fb.readDouble(f, r))
          }
          rowBatch += fb.rowCount
        }
      }
      // clean fbs
      elements.foreach { i =>
        JvmFrameStore.remove(i._1)
      }
      Iterator(fbs)
    })
    cacheStore
  }

  @deprecated
  def combineDoubleFbsBatch(store: ErStore): ErStore = {
    val partitions = if (checkRollMode(store)) {
      1
    } else {
      if (store.partitions.length < eggProcessors.length) {
        store.partitions.length
      } else {
        eggProcessors.length
      }
    }
    val cacheStore = createStore(store.storeLocator.namespace, "all_" + store.storeLocator.name, StringConstants.CACHE, partitions)
    // repartition
    val dir = FrameStore.getStoreDir(store)
    load(cacheStore).genData(_ => {
      // get all FrameBatch
      val elements = JvmFrameStore.getJvmFrameBatches(dir)
      val totalRows = elements.foldLeft(0)((a, b) => {
        a + b._2.head.rowCount
      })
      val fbs = new ListBuffer[FrameBatch]

      elements.foreach { i =>
        fbs.+=(i._2.head)
      }
      fbs.iterator
    })
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

  def getSession: ErSession = session

  def getSessionId: String = session.sessionId

  def stopSession(): Unit = {
    logger.info(s"Stop EggFrame Session:${session.sessionId}")
    session.clusterManagerClient.stopSession(session.sessionMeta)
  }

  def checkRollMode(store: ErStore): Boolean = {
    val rollServerId = store.partitions(0).processor.serverNodeId
    var res = true
    store.partitions.foreach { i =>
      if (rollServerId != i.processor.serverNodeId)
        res = false
    }
    res
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
                  singleThread: Boolean = true): ErStore = {
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
        val _processors = if (singleThread) {
          assignProcessors(serverNodes, totalPartitions)
        } else {
          assignProcessors(eggProcessors, totalPartitions)
        }
        (0 until totalPartitions).map { i =>
          ErPartition(id = i, storeLocator = storeLocator, processor = _processors(i))
        }.toArray
    }
    val store = ErStore(storeLocator = storeLocator, partitions = partitions)
    store
  }

  def forkStore(oldStore: ErStore, namespace: String, name: String, storeType: String = defaultStoreType,
                roll: Boolean = false): ErStore = {
    val storeLocator: ErStoreLocator = oldStore.storeLocator.copy(namespace = namespace, name = name, storeType = storeType,
      totalPartitions = oldStore.storeLocator.totalPartitions)
    val partitions: Array[ErPartition] = oldStore.partitions.map(i => i.copy(storeLocator = storeLocator))
    val store = ErStore(storeLocator = storeLocator, partitions = partitions)
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
    processorsCounts.indices.flatMap(i => Array.fill(processorsCounts(i))(processors(i))).toArray
    //    var res = Array[ErProcessor]()
    //    (0 until nodesLength).foreach(i => res = res ++ Array.fill(processorsCounts(i))(processors(i)))
  }

  def broadcast(path: String, frameBatch: FrameBatch): Unit = {
    frameTransfer.broadcast(path, frameBatch)
  }

  def scatter(path: String, frameBatch: FrameBatch): Unit = {
    frameTransfer.scatter(path, frameBatch)
  }
}

object RollFrameSession {
  val opts = Map("processor_types" -> "egg_frame", "processor_plan.egg_frame" -> "uniform")

  def getOrCreateNewSession(oldSessionId: String, sessionIdPrefix: String = ""): ErSession = {
    val oldSession = new ErSession(oldSessionId, options = opts)
    if (oldSession.sessionMeta.status == SessionStatus.ACTIVE) {
      oldSession
    } else {
      if (sessionIdPrefix.isEmpty) new ErSession(options = opts)
      else new ErSession(sessionId = s"${sessionIdPrefix}_${TimeUtils.getNowMs()}", options = opts)
    }
  }

  def createSession(sessionIdPrefix: String = ""): ErSession = {
    if (sessionIdPrefix.isEmpty) new ErSession(options = opts)
    else new ErSession(sessionId = s"${sessionIdPrefix}_${TimeUtils.getNowMs()}", options = opts)
  }
}

object RollFrameContext extends {
  //  StaticErConf.addProperties("conf/eggroll.properties")
  val opts = Map("processor_types" -> "egg_frame", "processor_plan.egg_frame" -> "uniform")

  def apply(session: ErSession): RollFrameContext = {
    new RollFrameContext(session)
  }

  def apply(sessionIdPrefix: String): RollFrameContext = {
    apply(new ErSession(sessionId = s"${sessionIdPrefix}_${TimeUtils.getNowMs()}", options = opts))
  }

  def apply(): RollFrameContext = {
    apply(new ErSession(options = opts))
  }

  def printSession(session: ErSession): Unit = {
    val sb = new StringBuilder
    sb.append("sessionId:").append(session.sessionId).append(",").append("processors:").
      append(session.processors.mkString(","))
    println(sb.toString())
  }
}

// create a instance when start a new job
// TODO: reuse ErJob generate and separate client mode and cluster mode
class RollFrame private[eggroll](val store: ErStore, val ctx: RollFrameContext) {
  val serdes = new DefaultScalaSerdes
  val rfScheduler = new RollFrameScheduler(ctx.session)

  private val seqJobId = new AtomicInteger()
  private val jobIdDf = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")

  def genJobId(): String = {
    jobIdDf.format(new Date()) + "_" + seqJobId.incrementAndGet()
  }

  def torchMap(path: String, parameters: Array[Double], output: ErStore = null): RollFrame = {
    val func: FrameBatch => FrameBatch = { fb =>
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
            ctx.logInfo("run merge ....")
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
    val func: FrameBatch => FrameBatch = { fb =>
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
    val func: FrameBatch => FrameBatch = { fb =>
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
      val inputIterator = if (task.name.equals(RollFrame.genData)) {
        Iterator(FrameUtils.mockEmptyBatch)
      } else {
        inputDB.readAll()
      }
      val ret = func(ctx, task, inputIterator, outputDB)
      ctx.logInfo(s"""finish runUnary ${task.job.name}, input: $inputPartition""")
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

  // There are two way to send command to  each processor.
  // 1. use runUnaryJob
  // 2. use transfer
  /**
   * execute command in each processor and no output.
   *
   * @param f functor
   */
  def mapCommand(f: FrameBatch => Unit): Unit = {
    val jobType = RollFrame.mapCommand
    val jobId = jobType + "_" + genJobId()

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        try {
          f(input.next())
        } catch {
          case e: Throwable => throw new RuntimeException(s"mapCommand task failed,task:$task", e)
        }
        null
    }
    runUnaryJob(RollFrame.mapCommand, func, jobId = jobId)
  }

  /**
   * thd method don't have return value, can set a fix variable
   *
   * @param code Python code
   */
  @deprecated
  def runPythonDistributedMock(code: String): Unit = {
    val jobType = RollFrame.embeddedPython
    val jobId = jobType + "_" + genJobId()
    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        try {
          // ignore data input
          val interp = LocalThreadPythonInterp.interpreterThreadLocal.get()
          // TODO: host and ip can be config.
          ctx.eggProcessors(0).transferEndpoint.host
          interp.setValue("world_size", ctx.eggProcessors.length)
          interp.setValue("rank", task.inputs.head.id)
          interp.exec(code)
          // can get a state variable
          assert(interp.getValue("state").asInstanceOf[Long] == 0, "Some error in python interpreter,because get error state")
        } catch {
          case e: Throwable => throw new RuntimeException(s"mapCommand task failed,task:$task", e)
        }
        null
    }
    runUnaryJob(RollFrame.embeddedPython, func, jobId = jobId)
    println("finished run pytorch distributed model")
  }

  // TODO: set private mothod
  /**
   * Embedding python interpreter to run python code with data from.
   *
   * @param code       : python code includeing ddp code. It better to wrap into function.
   *                   Tips:
   *                   1. input data need to cast to float32 using "torch.from_numpy(_input_data.astype(np.float32)).to('cpu')"
   *                      2. Must close process_group in python code, if want to reuse this interface.
   * @param field      : data Store's fields which will transfer to python interpreter
   * @param parameters : parameters given by user.
   * @param nonCopy    : use the same memory and don't copy data from direct buffer to heap buffer
   * @param output     : output ErStore
   */
  def runPythonDistributed(code: String,
                           field: Int,
                           parameters: Array[Double],
                           nonCopy: Boolean = false,
                           output: ErStore = null): Unit = {
    // to get available port
    val rollServerPort: Int = ctx.frameTransfer.Roll.getAvailablePort
    println(s"Roll Server available port: $rollServerPort")
    val jobType = RollFrame.embeddedPython
    val jobId = jobType + "_" + genJobId()
    val inputPartitions = store.partitions.length

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        try {
          //          println(Thread.currentThread().getName)
          // todo: Whether to set the python variable to global is uncertain，because two plans all have some benefits.
          // input args of python interpreter: _world_size, _rank, _input_data, _parameters, _master_addr, _master_port
          // output args of python interpreter : _result, _state
          val interp = LocalThreadPythonInterp.interpreterThreadLocal.get()
          // default is the first server
          val rollServerIp = ctx.rootServer.transferEndpoint.host
          val start = System.currentTimeMillis()
          interp.setValue("_world_size", inputPartitions)
          ctx.logInfo(s"_world_size : $inputPartitions")
          interp.setValue("_rank", task.inputs.head.id)
          ctx.logInfo(s"_rank: ${task.inputs.head.id}")
          interp.setValue("_master_addr", rollServerIp)
          interp.setValue("_master_port", rollServerPort.toString)
          ctx.logInfo(s"_master_addr: $rollServerIp ,_master_port: $rollServerPort")

          val inputParameters = new NDArray[Array[Float]](parameters.map(_.toFloat), parameters.length)
          interp.setValue("_parameters", inputParameters)
          val inputData = if (nonCopy) {
            val fb = input.next()
            val data = fb.rootVectors(0).fieldVector.getDataBuffer.nioBuffer()
            data.order(ByteOrder.LITTLE_ENDIAN)
            val rows = fb.rowCount / field
            ctx.logInfo(s"_input_data dim : = [$rows,$field]")
            new DirectNDArray[DoubleBuffer](data.asDoubleBuffer(), rows, field)
          } else {
            val data = FrameUtils.toFloatArray(input.next().rootVectors(0))
            val rows = data.length / field
            ctx.logInfo(s"_input_data dim : = [$rows,$field]")
            new NDArray[Array[Float]](data, rows, field)
            new NDArray[Array[Float]](data)
          }
          interp.setValue("_input_data", inputData)
          println(s"set py value: ${System.currentTimeMillis() - start} ms")
          interp.exec(code)
          println(s"py time: ${System.currentTimeMillis() - start} ms")
          // can get a state variable
          assert(interp.getValue("_state").asInstanceOf[Long] == 0, "Some error in python interpreter,because get error state")
          val result = interp.getValue("_result").asInstanceOf[NDArray[Double]]
          val resData = result.getData.asInstanceOf[Array[Double]]
          val rootSchema = new FrameSchema(SchemaUtil.oneDoubleFieldSchema)
          val outFb = new FrameBatch(rootSchema, resData.length)
          FrameUtils.copyMemory(outFb.rootVectors(0), resData)
          output.append(outFb)
          output.close()
        }
        catch {
          case e: Throwable => throw new RuntimeException(s"runPythonDistributed task failed,task:$task", e)
        }
        null
    }
    runUnaryJob(RollFrame.embeddedPython, func, jobId = jobId, output = output)
    println("finished run pytorch distributed model.")
  }

  def genData(f: FrameBatch => Iterator[FrameBatch]): RollFrame = {
    val jobType = RollFrame.genData
    val jobId = jobType + "_" + genJobId()
    // input store equal ouput store
    val output: ErStore = store

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        try {
          output.writeAll(f(input.next()))
          output.close()
        } catch {
          case e: Throwable => throw new RuntimeException(s"genData task failed,task:$task", e)
        }
        null
    }

    runUnaryJob(RollFrame.genData, func, jobId = jobId, output = output)
  }

  def mapBatch(f: FrameBatch => FrameBatch, output: ErStore = null): RollFrame = {
    val jobType = RollFrame.mapBatch
    val jobId = jobType + "_" + genJobId()

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        //         for concurrent writing
        //        val queuePath = task.id + "-doing"
        //                 total mean batch size, if given more than one, it just get one.
        //                val queue = FrameStore.queue(queuePath, 1)
        //                input.foreach { fb =>
        //                  ctx.executorPool.submit(new Runnable {
        //                    override def run(): Unit = {
        //                      // check store partition
        //                      println(s"Map Task ${task.id}: start, check fb, ${fb.rowCount},${fb.fieldCount}")
        //                      val res = f(fb)
        //                      println(s"Map Task ${task.id}: finish")
        //                      queue.append(res)
        //                    }
        //                  })
        //                }
        //                output.writeAll(queue.readAll())

        // did't use queue
        try {
          output.append(f(input.next()))
          output.close()
        } catch {
          case e: Throwable => throw new RuntimeException(s"map task failed,task:$task", e)
        }
        null
    }
    runUnaryJob(RollFrame.mapBatch, func, jobId = jobId, output = output)
  }

  /**
   * Simple all reduce.
   * - Each processor's FrameBatch was't scattered to other processor and finish reduce operation.
   */
  def allReduce(f: (FrameBatch, FrameBatch) => FrameBatch, output: ErStore = null): RollFrame = {
    val jobType = RollFrame.allReduce
    val jobId = jobType + "_" + genJobId()
    // assume each process only has one data partition
    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        // 1.scatter
        val eggQueuePath = "scatter_" + jobId
        val localFrameBatch: FrameBatch = input.next()
        ctx.frameTransfer.scatter(eggQueuePath, localFrameBatch)
        // 2. while for scatter data and reduce
        val partitionNum = task.inputs.head.storeLocator.totalPartitions
        //        if (partition.id == 0) {
        val localQueue = FrameStore.queue(eggQueuePath, partitionNum)
        val resultIterator = localQueue.readAll()
        if (!resultIterator.hasNext) throw new IllegalStateException("empty result")
        var localBatch: FrameBatch = FrameUtils.fork(resultIterator.next())
        while (resultIterator.hasNext) {
          localBatch = f(localBatch, resultIterator.next())
        }
        output.append(localBatch)
        output.close()
        //        }
        null
    }
    runUnaryJob(RollFrame.allReduce, func, jobId = jobId, output = output)
  }


  // TODO: add reduce by rows operation
  /**
   * reduce frameBatchs between different partitions
   * eg:
   * 1 1 1   2 2 2   3 3 3
   * 1 1 1 + 2 2 2 = 3 3 3
   * 1 1 1   2 2 2   3 3 3
   *
   * @return RollFrame
   */
  def reduce(f: (FrameBatch, FrameBatch) => FrameBatch, output: ErStore = null): RollFrame = {
    aggregate(null, combOp = f, output = output)
  }

  // Todo: integration of mapReduce
  //  def MapReduce(mf: FrameBatch => FrameBatch,f: (FrameBatch, FrameBatch) => FrameBatch, output: ErStore = null): RollFrame = {
  //    aggregateOp(null, f, f, mf, output = output)
  //  }

  def simpleAggregate(zeroValue: FrameBatch,
                      seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                      combOp: (FrameBatch, FrameBatch) => FrameBatch,
                      output: ErStore = null): RollFrame = {
    aggregate(zeroValue, seqOp = seqOp, combOp = combOp, output = output)
  }

  def aggregate(zeroValue: FrameBatch,
                broadcastZeroValue: Boolean = false,
                commParaOp: (FrameBatch, FrameBatch) => Map[String, Any] = (_, _) => Map(),
                seqOp: (FrameBatch, FrameBatch) => FrameBatch = null,
                seqByColumnOp: (FrameBatch, FrameBatch, Map[String, Any]) => Unit = null,
                seqParallel: Int = -1,
                seqByZeroValue: Boolean = false,
                combOp: (FrameBatch, FrameBatch) => FrameBatch,
                output: ErStore = null
               ): RollFrame = {
    aggregateOp(zeroValue, broadcastZeroValue, null, commParaOp, seqOp, seqByColumnOp, seqParallel, seqByZeroValue, combOp, output)
  }

  private def aggregateOp(zeroValue: FrameBatch,
                          broadcastZeroValue: Boolean = false,
                          genZeroValue: FrameBatch => FrameBatch = null,
                          commParaOp: (FrameBatch, FrameBatch) => Map[String, Any] = (_, _) => Map(),
                          seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                          seqByColumnOp: (FrameBatch, FrameBatch, Map[String, Any]) => Unit = null,
                          seqParallel: Int,
                          seqByZeroValue: Boolean = false,
                          combOp: (FrameBatch, FrameBatch) => FrameBatch = null,
                          output: ErStore = null): RollFrame = {
    val jobType = RollFrame.aggregate
    val jobId = jobType + "_" + genJobId()
    val driverZeroPath = "broadcast_" + jobId
    val zeroValueBytes = if (broadcastZeroValue) {
      if (store.storeLocator.totalPartitions == 1) {
        ctx.frameTransfer.Roll.broadcast(driverZeroPath, zeroValue)
      } else {
        ctx.broadcast(driverZeroPath, zeroValue)
      }
      Array[Byte]()
    } else if (zeroValue != null) {
      FrameUtils.toBytes(zeroValue)
    } else {
      Array[Byte]()
    } // broadcast spend > 200ms

    val treeComb: (EggFrameContext, String, Int) => Iterator[FrameBatch] = {
      (ctx, queuePath, comQueueSize) =>
        if (comQueueSize != 0) {
          val latch = new CountDownLatch(comQueueSize)
          (0 until comQueueSize).foreach { i =>
            val iter = FrameStore.queue(queuePath, 2).readAll()
            val a = iter.next()
            val b = iter.next()
            ctx.executorPool.submit(new Callable[Unit] {
              override def call(): Unit = {
                try {
                  ctx.logInfo(s"tree comb $i: start")
                  val res = combOp(a, b)
                  ctx.logInfo(s"tree comb $i: finish")
                  FrameStore.queue(queuePath, -1).writeAll(Iterator(res))
                }
                catch {
                  case e: Throwable => e.printStackTrace()
                } finally {
                  latch.countDown()
                  b.clear()
                }
              }
            })
          }
          ctx.logInfo("wait for finish all comb")
          latch.await()
          ctx.logInfo("finish all comb")
        }
        FrameStore.queue(queuePath, 1).readAll()
    }

    val func: (EggFrameContext, ErTask, Iterator[FrameBatch], FrameStore) => ErPair = {
      (ctx, task, input, output) =>
        try {
          val broadcastTime = System.currentTimeMillis()
          val zeroValue: FrameBatch = if (zeroValueBytes.isEmpty) null else FrameUtils.fromBytes(zeroValueBytes)
          val partition = task.inputs.head
          val localQueuePath = task.id + "-doing"
          // TODO: more generally, like repartition?
          // TODO: route
          // get store/partition/server message
          val localServer = partition.processor

          ctx.logInfo(s"runAggregateBatch: jobId=${task.job.id}, partitionId=${partition.id}, root=${ctx.rootServer}")
          var localQueue: FrameStore = null
          var localParallel: Int = 1
          val zero: FrameBatch =
            if (zeroValue == null) {
              val eggZeroValue = "broadcast_" + jobId
              if (broadcastZeroValue) {
                try {
                  var accTime = 0
                  while (!JvmFrameStore.checkFrameBatch(eggZeroValue)) {
                    Thread.sleep(50)
                    accTime += 50
                    if (accTime >= RollFrame.BROADCAST_MAX_WAIT_TIME) {
                      throw new RuntimeException(s"Egg Nodes didn't receive broadcast:$eggZeroValue")
                    }
                  }
                  //                  FrameStore.cache(eggZeroValue).readOne()
                  FrameUtils.fork(FrameStore.cache(eggZeroValue).readOne())
                } catch {
                  case e: Throwable =>
                    e.printStackTrace()
                    throw new RuntimeException(s"get $eggZeroValue value failed", e)
                }
              } else {
                // reduce need't zero value
                if (input.hasNext) {
                  if (genZeroValue == null) input.next() else genZeroValue(input.next())
                } else {
                  null
                }
              }
            } else {
              zeroValue
            }
          ctx.logInfo("get zero succeed.")
          println(s"get zero time:${System.currentTimeMillis() - broadcastTime}")
          // TODO: more generally, like repartition?
          if (input.hasNext) {
            val fb = input.next()
            if (seqByColumnOp != null) {
              val commonParameter = commParaOp(zero, fb)
              val begin = System.currentTimeMillis()
              var parallel: Int = if (seqParallel < 0) {
                val availableProcessors = Runtime.getRuntime.availableProcessors() - 1
                availableProcessors
              } else {
                seqParallel
              }
              if (fb.fieldCount <= parallel) {
                parallel = fb.fieldCount
              }
              ctx.logInfo(s"seq parallel mode,num=$parallel")
              val futures = new ListBuffer[Future[Unit]]
              if (!seqByZeroValue) {
                val slices = ctx.sliceByColumn(fb, parallel)
                slices.foreach { inclusive: Inclusive =>
                  futures.append(ctx.executorPool.submit(() => {
                    seqByColumnOp(zero, fb.sliceByColumn(inclusive.start, inclusive.end), commonParameter)
                  }))
                  //                  new Thread() {
                  //                    override def run(): Unit = {
                  //                      try {
                  //                        seqByColumnOp(zero, fb.sliceByColumn(inclusive.start, inclusive.end), commonParameter)
                  //                      } catch {
                  //                        case e: Throwable => e.printStackTrace()
                  //                      } finally {
                  //                        latch.countDown()
                  //                      }
                  //                    }
                  //                  }.start()
                }
              } else {
                val slices = ctx.sliceByColumn(zero, parallel)
                slices.foreach { inclusive: Inclusive =>
                  // TODO: check thread pool
                  futures.append(ctx.executorPool.submit(() => {
                    seqByColumnOp(zero.sliceByColumn(inclusive.start, inclusive.end), fb, commonParameter)
                  }))
                }
              }
              futures.foreach { i =>
                i.get()
              }
              println(s"egg seq:${System.currentTimeMillis() - begin} ms")
              localQueue = FrameStore.queue(localQueuePath, 1)
              localQueue.append(zero)
            } else {
              // use muti-thread by rows ,for example,parallel = 2, 100 rows can split to [0,50] and [50,100]
              // specify thread num, if zero value is to large , copy too many zero value will cause OOM
              // TODO: whether care about memory state and then decide thread num.
              val parallel: Int = if (seqParallel < 0) {
                val availableProcessors = Runtime.getRuntime.availableProcessors() / 2 // testing suggestion
                val eachThreadCount = 1000
                val tmpParallel = fb.rowCount / eachThreadCount + 1
                if (tmpParallel < availableProcessors) tmpParallel else availableProcessors
                //                          availableProcessors
              } else {
                seqParallel
              }
              localParallel = parallel
              localQueue = FrameStore.queue(localQueuePath, parallel)

              ctx.logInfo(s"map parallel = $parallel")
              println(parallel)
              val futures = new ListBuffer[Future[Unit]]
              ctx.sliceByRow(parallel, fb).foreach { inclusive: Inclusive =>
                futures.append(ctx.executorPool.submit(() => {
                  val tmpZeroValue = FrameUtils.fork(zero)
                  val start = System.currentTimeMillis()
                  localQueue.append(seqOp(tmpZeroValue, fb.sliceByRow(inclusive.start, inclusive.end)))
                  ctx.logInfo(s"seqOp time: ${System.currentTimeMillis() - start}")
                }))
              }
              futures.foreach { i =>
                i.get()
              }
            }
          } else {
            // for reduce op
            localQueue = FrameStore.queue(localQueuePath, 1)
            localQueue.append(zero)
          }
          // caution: cannot use return, in val func: xx,
          // and cannot use def func(xx) in scala 2.11 because of serder problem
          if (localQueue != null) {
            // todo: local queue and result synchronization, maybe a countdown latch
            // parallel comb
            val localBatch = treeComb(ctx, localQueuePath, localParallel - 1).next()
            // serial comb
            //          val resultIterator = localQueue.readAll()
            //          if (!resultIterator.hasNext) throw new IllegalStateException("empty result")
            //          var localBatch: FrameBatch = resultIterator.next()
            //          while (resultIterator.hasNext) {
            //            localBatch = combOp(localBatch, resultIterator.next())
            //          }

            val transferQueueSize = task.job.inputs.head.storeLocator.totalPartitions - 1
            require(transferQueueSize > -1, s"""transferQueueSize:$transferQueueSize, task:$task""")
            // TODO: check asynchronous call
            val combTime = System.currentTimeMillis()
            val queuePath = "gather:" + task.job.id
            if (localServer.commandEndpoint.host.equals(ctx.rootServer.commandEndpoint.host)) {
              // the same root server
              if (partition.id == 0) {
                ctx.logInfo(s"transferQueueSize = ${transferQueueSize}")
                // parallel combine
                FrameStore.queue(queuePath, -1).append(localBatch)
                output.writeAll(treeComb(ctx, queuePath, transferQueueSize))
                //   serial combine
                //      for (tmp <- FrameStore.queue(queuePath, transferQueueSize).readAll()) {
                //            localBatch = combOp(localBatch, tmp)
                //       }
                //       output.append(localBatch)
                println(s"roll comb: ${System.currentTimeMillis() - combTime} ms")
              } else {
                // the same root server but different partition
                FrameStore.queue(queuePath, -1).writeAll(Iterator(localBatch))
              }
            } else {
              // TODO: combine in every processor, reduce time of network transfer
              val sendTime = System.currentTimeMillis()
              // - use different clients
              //              val ft = new NioFrameTransfer(ctx.serverNodes)
              //              ft.send(ctx.rootServer.id, queuePath, localBatch)
              // - use the same client
              ctx.frameTransfer.synchronized(ctx.frameTransfer.send(ctx.rootServer.id, queuePath, localBatch))
              println(s"send roll: ${System.currentTimeMillis() - sendTime} ms")
              localBatch.clear()
            }
            output.close()
          }
        } catch {
          case e: Throwable => throw new RuntimeException(s"aggregate task failed,task:$task", e)
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
  val mapCommand = "mapCommand"
  val embeddedPython = "embeddedPython"
  val reduce = "reduce"
  val genData = "genData"
  val allReduce = "allReduce"
  val aggregate = "aggregate"
  val broadcast = "broadcast"
  val mulMul = "mulMulTask"

  val BROADCAST_MAX_WAIT_TIME = 10000 // ms

  def apply(store: ErStore, ctx: RollFrameContext): RollFrame = new RollFrame(store, ctx)
}
