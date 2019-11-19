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

import com.webank.eggroll.core.command.{CommandRouter, CommandService, CommandURI}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.schedule.BaseTaskPlan
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.format.{FrameBatch, _}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder


// TODO: always close in finally
// TODO: Use a dag to express program with base plan like reading/writing/scatter/broadcast etc.

class AggregateBatchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class MapBatchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class ReduceBatchTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class MapPartitionTask(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)


trait RollFrame

// create a instance when start a new job
// TODO: reuse ErJob generate and separate client mode and cluster mode
class RollFrameClientMode(val store: ErStore) extends RollFrame {

  val serdes = new DefaultScalaSerdes

  val rollFrameService = new RollFrameService

  def mapBatch(f: FrameBatch => FrameBatch, output: ErStore = null): RollFrameClientMode = {
    val jobType = RollFrame.mapBatch
    val job = ErJob(id = jobType,
      // name = s"${RollFrame.rollFrame}.${RollFrame.mapBatch}",
      name = EggFrame.mapBatchTask,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      functors = Array(ErFunctor(name = RollFrame.mapBatch, body = serdes.serialize(f))))

    processJobResult(rollFrameService.mapBatches(job))
  }

  def reduce(f: (FrameBatch, FrameBatch) => FrameBatch, output: ErStore = null): RollFrameClientMode = {
    val jobType = RollFrame.reduce
    val job = ErJob(id = RollFrame.reduce,
      name = EggFrame.reduceTask,
      inputs = Array(store),
      outputs = Array(if (output == null) store.fork(postfix = jobType) else output),
      functors = Array(ErFunctor(name = RollFrame.reduce, body = serdes.serialize(f))))

    processJobResult(rollFrameService.reduce(job))
  }

  def aggregate(zeroValue: FrameBatch,
                seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                combOp: (FrameBatch, FrameBatch) => FrameBatch,
                byColumn: Boolean = false,
                broadcastZeroValue: Boolean = false,
                output: ErStore = null): RollFrameClientMode = {
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
        ErFunctor(name = "broadcastZeroValue", body = serdes.serialize(broadcastZeroValue))))

    processJobResult(rollFrameService.aggregate(job))
  }

  // todo: pull up

  def processJobResult(job: ErJob): RollFrameClientMode = {
    new RollFrameClientMode(job.outputs.head)
  }
}

object RollFrame {
  val rollFrame = "RollFrame"
  val eggFrame = "EggFrame"
  val mapBatch = "mapBatch"
  val reduce = "reduce"
  val aggregate = "aggregate"
}

// TODO: MOCK
class ClusterManager(mode: String = "local") {
  val clusterNode0 = ErProcessor(id = 0, commandEndpoint = ErEndpoint("node1", 20100), dataEndpoint = ErEndpoint("node1", 20200), tag = "boss")
  val clusterNode1 = ErProcessor(id = 1, commandEndpoint = ErEndpoint("node2", 20101), dataEndpoint = ErEndpoint("node2", 20201), tag = "worker")
  val clusterNode2 = ErProcessor(id = 2, commandEndpoint = ErEndpoint("node3", 20102), dataEndpoint = ErEndpoint("node3", 20202), tag = "worker")

  val localNode0 = ErProcessor(id = 0, commandEndpoint = ErEndpoint("127.0.0.1", 20100), dataEndpoint = ErEndpoint("127.0.0.1", 20200), tag = "boss")
  val localNode1 = ErProcessor(id = 1, commandEndpoint = ErEndpoint("127.0.0.1", 20101), dataEndpoint = ErEndpoint("127.0.0.1", 20201), tag = "worker")
  def getLiveProcessorBatch(clusterId: Long = -1): ErProcessorBatch = {
    val cluster = mode match {
      case "cluster" =>
        ErProcessorBatch(id = clusterId, processors = Array(clusterNode0, clusterNode1, clusterNode2))
      case _ => ErProcessorBatch(id = clusterId, processors = Array(localNode0, localNode1))
    }
    cluster
  }

  def getRollFrameStore(name: String, namespace: String): ErStore = {
    // TODO:How to get partition num, frameBatch count?
    val storeLocator = ErStoreLocator(
      storeType = StringConstants.FILE,
      namespace = namespace,
      name = name)
    val partitions = mode match {
      case "cluster" => Array(
        ErPartition(id = 0, storeLocator = storeLocator, processor = clusterNode0),
        ErPartition(id = 1, storeLocator = storeLocator, processor = clusterNode1),
        ErPartition(id = 2, storeLocator = storeLocator, processor = clusterNode2)
      )
      case _ => Array(
        ErPartition(id = 0, storeLocator = storeLocator, processor = localNode0),
        ErPartition(id = 1, storeLocator = storeLocator, processor = localNode1))
    }
    ErStore(storeLocator = storeLocator, partitions = partitions)
  }

  def getPreferredServer(store: ErStore, clusterId: Long = -1): Map[Int, ErProcessor] = {
    val nodes = getLiveProcessorBatch(clusterId).processors

    nodes.indices.zip(nodes).toMap
  }

  def startServerCluster(clusterId: Long = -1, nodeId: Long = -1): Unit = {

    CommandRouter.register(
      serviceName = "EggFrame.runTask",
      serviceParamTypes = Array(classOf[ErTask]),
      serviceResultTypes = Array(classOf[ErTask]),
      routeToClass = classOf[EggFrame],
      routeToMethodName = "runTask")

    getLiveProcessorBatch(clusterId).processors.foreach { server =>
      val idMatch = mode match {
        case "cluster" => server.id == nodeId
        case _ => true
      }
      val commandEndpoint = server.commandEndpoint
      val dataEndpoint = server.dataEndpoint
      if (idMatch) {
        val sb = NettyServerBuilder.forPort(commandEndpoint.port)
        sb.addService(new CommandService).build.start
        println("Start GrpcCommandService...")
        new Thread("transfer-" + dataEndpoint.port) {
          override def run(): Unit = {
            try {
              println(s"Start TransferServer:server.host: ${server.dataEndpoint.host}, transferPost: ${server.dataEndpoint.port}")
              new NioTransferEndpoint().runServer(server.dataEndpoint.host, server.dataEndpoint.port)
            } catch {
              case e: Throwable => e.printStackTrace()
            }
          }
        }.start()
      }
    }
  }
}

object ClusterManager {
  def getOrCreate(): ClusterManager = new ClusterManager
}
