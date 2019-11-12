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

import java.io._
import java.net.URI
import java.util.Random
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.command.{CommandRouter, CommandService, CommandURI}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.schedule.{BaseTaskPlan, JobRunner, ListScheduler}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.format.{FrameBatch, FrameDB, _}
import io.grpc.ServerBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


// TODO: always close in finally

//import com.webank.eggroll.rollframe.MessageSerializer._

/*
// TODO: Use a dag to express program with base plan like reading/writing/scatter/broadcast etc.
// A program packages all jobs/tasks to optimize together
class ExecuteProgram {

  private val plans:ListBuffer[TaskPlan] = ListBuffer[TaskPlan]()
  private val functorSerdes = ScalaFunctorSerdes
  private def newTaskId(jobId: String) = jobId + "-task" + Math.abs(new Random().nextLong())

  private def newJobId() = "job" + Math.abs(new Random().nextLong())

  def addPlan(plan: TaskPlan):ExecuteProgram = {
    plans.append(plan)
    this
  }

  def build(inputs: List[RfStore], clusterManager: ClusterManager,
            outputs: List[RfStore]): List[EndpointCommand] = {
    // only one input&output&plan supported now
    val input = inputs.head
    val plan = plans.head
    val jobId = newJobId()
    val output = outputs.head.copy(partitions = null)
    val partitionRoutes = clusterManager.getPreferredServer(input)
    val job = RfJob(jobId, partitionRoutes.size, List(output))
    def getTask(taskType:String, part: RfPartition, functors: List[(String, Any)]): RfTask = {
      val funcs = functors.map { case (name, f) =>
        RfFunctor(name, f match {
          case bytes:Array[Byte] => bytes
          case _ => functorSerdes.serialize(f)
        })
      }
      part.store = input.copy(partitions = null)
      RfTask(newTaskId(jobId),taskType, funcs, List(part), job)
    }

    input.partitions.flatMap{ part =>
      val rfTasks = plan match {
        case task: AggregateBatchTask =>
          val zero = if(task.broadcastZero || task.byColumn) Array[Byte]() else FrameUtil.toBytes(task.zeroValue)
          var tasks = getTask("AggregateBatchTask", part,
            List(("zeroValue", zero), ("seqOp", task.seqOp), ("combOp", task.combOp), ("byColumn", task.byColumn))
          ) :: Nil
          val zeroPath = "broadcast:" + jobId
          // Write to current server node's store for self using and transfer
          FrameDB.cache(zeroPath)
            .writeAll(Iterator(task.zeroValue))
          if(part.id == 0 && task.broadcastZero) {
            // TODO: scatter zero value when aggregating byColumn
            tasks ::= getTask("BroadcastTask", part, List(("broadcast", zeroPath)))
          }
          tasks
        case task: MapBatchTask => getTask("MapBatchTask",part, List(("mapBatches", task.mapper))) :: Nil
        case task: ReduceBatchTask => getTask("ReduceBatchTask",part, List(("mapBatches", task.reducer))) :: Nil
        case _ => throw new UnsupportedOperationException("unsupported task")
      }
      // TODO: command service should merge the same endpoint's commands to one. And use local invoking instead of network
      rfTasks.map(t => EndpointCommand(
        new URI("/grpc/v1?route=com.webank.eggroll.rollframe.EggFrame.runTask"),
        t.toBytes(), null, partitionRoutes(part.id)))
    }
  }
}

trait TaskPlan
case class AggregateBatchTask(zeroValue:FrameBatch, seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                    combOp: (FrameBatch, FrameBatch) => FrameBatch, byColumn:Boolean = false,
                    broadcastZero:Boolean = true) extends TaskPlan
case class MapBatchTask(mapper: FrameBatch => FrameBatch) extends TaskPlan
case class ReduceBatchTask(reducer:(FrameBatch, FrameBatch) => FrameBatch) extends TaskPlan
case class MapPartitionTask(mapper:Iterator[FrameBatch] => Iterator[FrameBatch]) extends TaskPlan
*/


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
      inputs = List(store),
      outputs = List(if (output == null) store.fork(postfix = jobType) else output),
      functors = List(ErFunctor(name = RollFrame.mapBatch, body = serdes.serialize(f))))

    new RollFrameClientMode(rollFrameService.mapBatches(job))
  }

  def reduce(f: (FrameBatch, FrameBatch) => FrameBatch, output: ErStore = null): RollFrameClientMode = {
    val jobType = RollFrame.reduce
    val job = ErJob(id = RollFrame.reduce,
      name = EggFrame.reduceTask,
      inputs = List(store),
      outputs = List(if (output == null) store.fork(postfix = jobType) else output),
      functors = List(ErFunctor(name = RollFrame.reduce, body = serdes.serialize(f))))

    new RollFrameClientMode(rollFrameService.reduce(job))
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
      inputs = List(store),
      outputs = List(if (output == null) store.fork(postfix = jobType) else output),
      // todo: broadcast of zeroValue needs another implementation because there is a gRPC message size limit here
      functors = List(ErFunctor(name = "zeroValue", body = FrameUtils.toBytes(zeroValue)),
        ErFunctor(name = "seqOp", body = serdes.serialize(seqOp)),
        ErFunctor(name = "combOp", body = serdes.serialize(combOp)),
        ErFunctor(name = "byColumn", body = serdes.serialize(byColumn)),
        ErFunctor(name = "broadcastZeroValue", body = serdes.serialize(broadcastZeroValue))))

    new RollFrameClientMode(rollFrameService.aggregate(job))
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
  val clusterNode0 = ErServerNode(id = "0", commandEndpoint = ErEndpoint("node1", 20100), dataEndpoint = ErEndpoint("node1", 20200), tag = "boss")
  val clusterNode1 = ErServerNode(id = "1", commandEndpoint = ErEndpoint("node2", 20101), dataEndpoint = ErEndpoint("node2", 20201), tag = "worker")
  val clusterNode2 = ErServerNode(id = "2", commandEndpoint = ErEndpoint("node3", 20102), dataEndpoint = ErEndpoint("node3", 20202), tag = "worker")

  val localNode0 = ErServerNode(id = "0", commandEndpoint = ErEndpoint("127.0.0.1", 20100), dataEndpoint = ErEndpoint("127.0.0.1", 20200), tag = "boss")
  val localNode1 = ErServerNode(id = "1", commandEndpoint = ErEndpoint("127.0.0.1", 20101), dataEndpoint = ErEndpoint("127.0.0.1", 20201), tag = "worker")
  def getServerCluster(clusterId: String = null): ErServerCluster = {
    val cluster = mode match {
      case "cluster" =>
        ErServerCluster(id = clusterId, nodes = List(clusterNode0, clusterNode1, clusterNode2))
      case _ => ErServerCluster(clusterId, List(localNode0, localNode1))
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
      case "cluster" => List(
        ErPartition(id = "0", storeLocator = storeLocator, node = clusterNode0),
        ErPartition(id = "1", storeLocator = storeLocator, node = clusterNode1),
        ErPartition(id = "2", storeLocator = storeLocator, node = clusterNode2)
      )
      case _ => List(
        ErPartition(id = "0", storeLocator = storeLocator, node = localNode0),
        ErPartition(id = "1", storeLocator = storeLocator, node = localNode1))
    }
    ErStore(storeLocator = storeLocator, partitions = partitions)
  }

  def getPreferredServer(store: ErStore, clusterId: String = null): Map[Int, ErServerNode] = {
    val nodes = getServerCluster(clusterId).nodes

    nodes.indices.zip(nodes).toMap
  }

  def startServerCluster(clusterId: String = null, nodeId: String): Unit = {

    CommandRouter.register(
      serviceName = "EggFrame.runTask",
      serviceParamTypes = List(classOf[ErTask]),
      serviceReturnTypes = List(classOf[ErStore]),
      routeToClass = classOf[EggFrame],
      routeToMethodName = "runTask")

    getServerCluster(clusterId).nodes.foreach { server =>
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
