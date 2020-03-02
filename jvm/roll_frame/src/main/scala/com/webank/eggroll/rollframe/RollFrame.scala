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

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.schedule.BaseTaskPlan
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.format.{FrameBatch, _}

// TODO: care about client task grpc whether closed and thread pool whether closed
// TODO: always close in finally
// TODO: Use a dag to express program with base plan like reading/writing/scatter/broadcast etc.

class AggregateBatchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)

class MapBatchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)

class ReduceBatchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)

class MapPartitionTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)

class TorchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)

class RollFrameContext private[eggroll](val session: ErSession) {
  def load(store: ErStore):RollFrame = RollFrame(store, this)
}
object RollFrameContext {
  StaticErConf.addProperties("conf/eggroll.properties")
  def apply(session: ErSession): RollFrameContext = new RollFrameContext(session)
  def apply(): RollFrameContext = {
    val opts = Map("processor_types" -> "egg_frame", "processor_plan.egg_frame" -> "uniform")
      apply(new ErSession(options = opts))
  }
}
// create a instance when start a new job
// TODO: reuse ErJob generate and separate client mode and cluster mode
class RollFrame private[eggroll](val store: ErStore, val ctx: RollFrameContext) {

  val serdes = new DefaultScalaSerdes
  val rfScheduler = new RollFrameScheduler(ctx.session)

  @deprecated
  def matMulV1(m: Array[Double], rows: Int, cols: Int, output: ErStore = null): RollFrame = {
    val jobType = RollFrame.mulMul
    val job = ErJob(id = jobType,
      name = EggFrame.mulMulTaskV1,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      functors = Array(ErFunctor(name = RollFrame.mulMul, body = serdes.serialize(m)),
        ErFunctor(name = "rows", body = serdes.serialize(rows)),
        ErFunctor(name = "cols", body = serdes.serialize(cols)))
    )
    processJobResult(rfScheduler.mulMul(job))
  }

  @deprecated
  def matMul(m: Array[Double], rows: Int, cols: Int, output: ErStore = null): RollFrame = {
    val jobType = RollFrame.mulMul
    val job = ErJob(id = jobType,
      name = EggFrame.mulMulTask,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      functors = Array(ErFunctor(name = RollFrame.mulMul, body = serdes.serialize(m)),
        ErFunctor(name = "rows", body = serdes.serialize(rows)),
        ErFunctor(name = "cols", body = serdes.serialize(cols)))
    )
    processJobResult(rfScheduler.mulMul(job))
  }

  def mapBatch(f: FrameBatch => FrameBatch, output: ErStore = null): RollFrame = {
    val jobType = RollFrame.mapBatch
    val job = ErJob(id = jobType,
      // name = s"${RollFrame.rollFrame}.${RollFrame.mapBatch}",
      name = EggFrame.mapBatchTask,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      functors = Array(ErFunctor(name = RollFrame.mapBatch, body = serdes.serialize(f))))

    processJobResult(rfScheduler.mapBatches(job))
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
    val jobType = RollFrame.reduce
    val job = ErJob(id = RollFrame.reduce,
      name = EggFrame.reduceTask,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      functors = Array(ErFunctor(name = RollFrame.reduce, body = serdes.serialize(f))))

    processJobResult(rfScheduler.reduce(job))
  }

  def aggregate(zeroValue: FrameBatch,
                seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                combOp: (FrameBatch, FrameBatch) => FrameBatch,
                byColumn: Boolean = false,
                broadcastZeroValue: Boolean = false,
                threadsNum: Int = -1,
                output: ErStore = null): RollFrame = {
    val jobType = RollFrame.aggregate
    val job = ErJob(id = RollFrame.aggregate,
      name = EggFrame.aggregateBatchTask,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      // todo: broadcast of zeroValue needs another implementation because there is a gRPC message size limit here
      functors = Array(ErFunctor(name = "zeroValue", body = FrameUtils.toBytes(zeroValue)),
        ErFunctor(name = "seqOp", body = serdes.serialize(seqOp)),
        ErFunctor(name = "combOp", body = serdes.serialize(combOp)),
        ErFunctor(name = "byColumn", body = serdes.serialize(byColumn)),
        ErFunctor(name = "broadcastZeroValue", body = serdes.serialize(broadcastZeroValue)),
        ErFunctor(name = "parallel", body = serdes.serialize(threadsNum))))
    processJobResult(rfScheduler.aggregate(job))
  }

  // todo: pull up

  def processJobResult(job: ErJob): RollFrame = {
    ctx.load(job.outputs.head)
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
