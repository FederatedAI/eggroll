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

package com.webank.eggroll.rollframe

import java.net.BindException

import com.webank.eggroll.core.{Bootstrap, ErSession}
import com.webank.eggroll.core.constant.{ProcessorStatus, ProcessorTypes, StringConstants}
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.format.FrameStore

object TestAssets extends Serializable {

  val DELTA = 0.0000001
  StaticErConf.addProperty("eggroll.resourcemanager.clustermanager.host", "127.0.0.1")
  StaticErConf.addProperty("eggroll.resourcemanager.clustermanager.port", "4670")
  StaticErConf.addProperty("hadoop.fs.defaultFS", "file:///")

  def getRfContext(isDebug: Boolean = true, withRm: Boolean = false, prefix: String = ""): RollFrameContext = {
    if (!isDebug) {
      if (prefix.isEmpty) {
        RollFrameContext(sessionIdPrefix = prefix)
      } else RollFrameContext()
    } else {
      if (withRm) {
        new Thread("eggroll-rm-bootstrap") {
          override def run(): Unit = {
            Bootstrap.main(s"--config conf/eggroll.properties --bootstraps com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap,com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap -s debug-sid -p 4670 --ignore-rebind".split(" "))
          }
        }.start()
        Thread.sleep(3000)
      }
      new Thread("eggroll-ef-bootstrap") {
        override def run(): Unit = {
          Bootstrap.main(s"--config conf/eggroll.properties --bootstraps com.webank.eggroll.rollframe.EggFrameBootstrap -s debug-sid -p 20100  -tp 20200".split(" "))
        }
      }.start()

      RollFrameContext(new ErSession(sessionId = "debug-sid",
        processors = getLiveProcessorBatch().processors))
    }
  }

  /**
   * mock -- use to load Cache on local mode
   *
   * @param inStore ErStore
   * @return
   */
  def loadCache(inStore: ErStore): ErStore = {
    val cacheStoreLocator = inStore.storeLocator.copy(storeType = StringConstants.CACHE)
    val cacheStore = inStore.copy(storeLocator = cacheStoreLocator, partitions = inStore.partitions.map(p =>
      p.copy(storeLocator = cacheStoreLocator)))
    inStore.partitions.indices.foreach { i =>
      val inputDB = FrameStore(inStore, i)
      val outputDB = FrameStore(cacheStore, i)
      outputDB.writeAll(inputDB.readAll())
      inputDB.close()
      outputDB.close()
    }
    cacheStore
  }

  val localNode0: ErProcessor = ErProcessor(id = 0, serverNodeId = 0, commandEndpoint = ErEndpoint("127.0.0.1", 20100), transferEndpoint = ErEndpoint("127.0.0.1", 20200), status = ProcessorStatus.RUNNING, processorType = ProcessorTypes.EGG_FRAME)
  val localNode1: ErProcessor = ErProcessor(id = 1, serverNodeId = 1, commandEndpoint = ErEndpoint("127.0.0.1", 20100), transferEndpoint = ErEndpoint("127.0.0.1", 20201), status = ProcessorStatus.RUNNING, processorType = ProcessorTypes.EGG_FRAME)

  def getLiveProcessorBatch(clusterId: Long = -1): ErProcessorBatch = {
    ErProcessorBatch(id = clusterId, processors = Array(localNode0))
  }

  def getRollFrameStore(name: String, namespace: String, storeType: String = StringConstants.FILE,
                        processorCount: Int = 1): ErStore = {
    // TODO:How to get partition num, frameBatch count?
    require(processorCount == 1, s"unsupported processorCount: ${processorCount}")
    val storeLocator = ErStoreLocator(
      storeType = storeType, totalPartitions = 3,
      namespace = namespace,
      name = name)
    val partitions = Array(
      ErPartition(id = 0, storeLocator = storeLocator, processor = localNode0),
      ErPartition(id = 1, storeLocator = storeLocator, processor = localNode0),
      ErPartition(id = 2, storeLocator = storeLocator, processor = localNode0))
    ErStore(storeLocator = storeLocator, partitions = partitions)
  }

  def getPreferredServer(store: ErStore, clusterId: Long = -1): Map[Int, ErProcessor] = {
    val nodes = getLiveProcessorBatch(clusterId).processors

    nodes.indices.zip(nodes).toMap
  }

  def getSchema(fieldCount: Int): String = {
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
