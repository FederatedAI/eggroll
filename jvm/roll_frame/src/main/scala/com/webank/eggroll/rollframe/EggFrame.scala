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
import com.webank.eggroll.core.meta.{ErPair, ErProcessor, ErSessionMeta, ErTask}
import com.webank.eggroll.core.serdes.{DefaultScalaSerdes, MonadSerDes}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.{Logging, ThreadPoolUtils}
import com.webank.eggroll.format.FrameBatch

import scala.collection.immutable.Range.Inclusive

class EggFrameContext extends Logging {
  val serdes: MonadSerDes = DefaultScalaSerdes()
  val executorPool: ThreadPoolExecutor = ThreadPoolUtils.defaultThreadPool
  lazy val eggProcessors: Array[ErProcessor] = EggFrame.session.processors
  lazy val frameTransfer: FrameTransfer = new NioFrameTransfer(eggProcessors)
  lazy val rootServer: ErProcessor = eggProcessors.head

  override def logInfo(msg: => String): Unit = super.logInfo(msg)

  @deprecated("eggProcessors before are serverNodes,and didn't check")
  def sliceByColumn(frameBatch: FrameBatch): List[(ErProcessor, Inclusive)] = {
    val columns = frameBatch.rootVectors.length
    val servers = eggProcessors.length
    val partSize = (servers + columns - 1) / servers
    (0 until servers).map { sid =>
      (eggProcessors(sid), new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, columns), 1))
    }.toList
  }

  def sliceByColumn(frameBatch: FrameBatch, parallel: Int): List[Inclusive] = {
    val columns = frameBatch.rootVectors.length
//    assert(columns > parallel, "FrameBatch's fields counts smaller than parallel num.")
    val quotient = columns / parallel
    val remainder = columns % parallel
    val processorsCounts = Array.fill(parallel)(quotient)
    (0 until remainder).foreach(i => processorsCounts(i) += 1)
    var start = 0
    var end = 0
    processorsCounts.map { count =>
      end = start + count
      val range = new Inclusive(start, end, 1)
      start = end
      range
    }.toList
  }

  def sliceByRow(parts: Int, frameBatch: FrameBatch): List[Inclusive] = {
    val rows = frameBatch.rowCount
    val partSize = (parts + rows - 1) / parts
    (0 until parts).map { sid =>
      new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, rows), 1)
    }.toList
  }
}

class EggFrame {
  private val ctx = new EggFrameContext()

  def runTask(task: ErTask): ErPair = {
    val func: (EggFrameContext, ErTask) => ErPair = ctx.serdes.deserialize(task.job.functors.head.body)
    func(ctx, task)
  }
}

object EggFrame {
  private[eggroll] lazy val session = new ClusterManagerClient().getSession(ErSessionMeta(
    id = StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_ID, null)))
}
