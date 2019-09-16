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

import java.io._
import java.net.URI
import java.util.Random
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.blockstore._
import com.webank.eggroll.command.{CollectiveCommand, EndpointCommand}
import com.webank.eggroll.format._
import com.webank.eggroll.transfer.{CollectiveTransfer, GrpcTransferService}

import scala.reflect.ClassTag


// TODO: always close in finally

trait RpcMessage

case class ServerNode(host: String, port: Int, tag: String, id: String = "") extends RpcMessage

case class ServerCluster(id: String, nodes: List[ServerNode]) extends RpcMessage

case class RfPartition(id: Int, var store: RfStore = null) extends RpcMessage

case class RfStore(name: String, namespace: String, partitions: List[RfPartition] = List[RfPartition](),
                   path: String = null, adapter: String = "file") extends RpcMessage

case class RfJob(jobId: String, outputs: List[RfStore] = List[RfStore]()) extends RpcMessage

case class RfFunctor(name: String, body: Array[Byte]) extends RpcMessage

case class RfTask(taskId: String, functors: List[RfFunctor], oprands: List[RfPartition], job: RfJob) extends RpcMessage

trait MessageSerializer {
  def toBytes(): Array[Byte]
}

trait MessageDeserializer {
  def fromBytes[T: ClassTag](bytes: Array[Byte]): T
}

trait PbMessageSerializer extends MessageSerializer {
  def toProto[T >: PbMessage](): T

  override def toBytes(): Array[Byte] = toProto().toByteArray // toByteString preferred
  def toByteString(): ByteString = toProto().toByteString
}

trait PbMessageDeserializer extends MessageDeserializer {
  def fromProto[T >: RpcMessage](): T

  def fromBytes[T: ClassTag](bytes: Array[Byte]): T = ???
}

object MessageSerializer {

  implicit class RfPartitionSerializer(rfPartition: RfPartition) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): RollFrameGrpc.Partition = {
      val b = RollFrameGrpc.Partition.newBuilder().setId(rfPartition.id.toString)
      if (rfPartition.store != null) b.setStore(rfPartition.store.toProto())
      b.build()
    }
  }

  implicit class RfPartitionDeserializer(p: RollFrameGrpc.Partition) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): RfPartition = RfPartition(p.getId.toInt, p.getStore.fromProto())
  }

  implicit class RfFunctorSerializer(rfFunctor: RfFunctor) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): RollFrameGrpc.Functor = {
      RollFrameGrpc.Functor.newBuilder().setName(rfFunctor.name).setBody(ByteString.copyFrom(rfFunctor.body)).build()
    }
  }

  implicit class RfFunctorDeserializer(p: RollFrameGrpc.Functor) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): RfFunctor = {
      RfFunctor(p.getName, p.getBody.toByteArray)
    }
  }

  implicit class RfTaskSerializer(rfTask: RfTask) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): T = {
      RollFrameGrpc.Task.newBuilder()
        .setJob(rfTask.job.toProto())
        .addAllFunctors(rfTask.functors.map(_.toProto()).asJava)
        .addAllOperands(rfTask.oprands.map(_.toProto()).asJava)
        .setTaskId(rfTask.taskId)
        .build()
    }
  }

  implicit class RfTaskDeserializer(p: RollFrameGrpc.Task) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): RfTask = {
      RfTask(p.getTaskId,
        p.getFunctorsList.asScala.map(_.fromProto()).toList,
        p.getOperandsList.asScala.map(_.fromProto()).toList,
        p.getJob.fromProto()
      )
    }
  }

  implicit class RfJobSerializer(rfJob: RfJob) extends PbMessageSerializer {

    override def toProto[T >: PbMessage](): RollFrameGrpc.Job = {
      RollFrameGrpc.Job.newBuilder().setJobId(rfJob.jobId).build()
    }
  }

  implicit class PbRfJob2(p: RollFrameGrpc.Job) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): RfJob =
      RfJob(p.getJobId, p.getOutputsList.asScala.map(_.fromProto()).toList)
  }

  implicit class RfStoreSerializer(rfStore: RfStore) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): RollFrameGrpc.Store = {
      val b = RollFrameGrpc.Store.newBuilder().setName(rfStore.name)
        .setNamespace(rfStore.namespace)
      if (rfStore.path != null) b.setPath(rfStore.path)
      b.build()
    }
  }

  implicit class RfStoreDeserializer(p: RollFrameGrpc.Store) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): RfStore = RfStore(p.getName, p.getNamespace, path = p.getPath)
  }

}


trait RollFrame {

}

// create a instance when start a new job
// TODO: rename RollFrameService
class RollFrameService(store: RfStore) extends RollFrame {

  import MessageSerializer._

  val clusterManager = new ClusterManager
  val collectiveCommand = new CollectiveCommand(clusterManager.getServerCluster("test1").nodes)
  val functorSerdes = ScalaFunctorSerdes

  private def newTaskId() = "task-" + Math.abs(new Random().nextLong())

  private def newJobId() = "job-" + "1"

  // TODO: MOCK
  //  private def newJobId() = "job-" + Math.abs(new Random().nextLong())

  private def getTask(jobId: String, part: RfPartition, functors: List[(String, Any)]): Array[Byte] = {
    val funcs = functors.map { case (name, f) =>
      RfFunctor(name, f match {
        case batch: FrameBatch => FrameUtil.toBytes(batch)
        case _ => functorSerdes.serialize(f)
      })
    }
    part.store = store.copy(partitions = null)
    RfTask(newTaskId(), funcs, List(part), RfJob(newJobId(), List(RfStore(newJobId(), "test1")))).toBytes()
  }

  private def getCmds(jobId: String, functors: List[(String, Any)]): List[EndpointCommand] = {
    store.partitions.map { part =>
      val task = getTask(jobId, part, functors)
      EndpointCommand(
        new URI("/grpc/v1?route=com.webank.eggroll.rollframe.EggFrame.runTask"), task, null)
    }
  }

  def mapBatches(f: FrameBatch => FrameBatch): RollFrame = {
    val jobId = newJobId()
    collectiveCommand.syncSendAll(getCmds(jobId, List(("mapBatches", f))))
    new RollFrameService(RfStore(jobId, "test1", store.partitions))
  }

  // TODO: disable reduce?
  def reduce(f: (FrameBatch, FrameBatch) => FrameBatch): RollFrame = {
    val jobId = newJobId()
    collectiveCommand.syncSendAll(getCmds(jobId, List(("reduce", f))))
    new RollFrameService(RfStore(jobId, "test1", List(RfPartition(0))))
  }

  def aggregate(zeroValue: FrameBatch,
                seqOp: (FrameBatch, FrameBatch) => FrameBatch,
                combOp: (FrameBatch, FrameBatch) => FrameBatch): RollFrame = {
    val jobId = newJobId()
    collectiveCommand.syncSendAll(
      getCmds(jobId, List(("zeroValue", zeroValue), ("seqOp", seqOp), ("combOp", combOp))))
    new RollFrameService(RfStore(jobId, "test1", List(RfPartition(0))))
  }

  //  def collect()
}

class EggFrame {
  val rootPath = "./tmp/unittests/RollFrameTests/filedb/"
  val clusterManager = new ClusterManager
  val serverNodes = clusterManager.getServerCluster().nodes

  import MessageSerializer._

  def runTask(pbTask: RollFrameGrpc.Task): RollFrameGrpc.TaskResult = {
    //    require(task.getFunctorsCount == 1 && task.getOperandsCount == 1, "todo")
    val task = pbTask.fromProto()
    val part = task.oprands.head
    val store = part.store
    val path = rootPath + Array(store.namespace, store.name, part.id).mkString("/")

    println("inputPath", path)

    val tmpOutName = pbTask.getJob.getJobId
    val outputPath = rootPath + Array(store.namespace, store.name + "-tmp-" + tmpOutName, part.id).mkString("/")
    println("outputPath", outputPath)
    //  a stream => a thread
    val executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque[Runnable](100))
    val resultQueue = new LinkedBlockingQueue[FrameBatch]()
    val inputOpts = Map("path" -> path, "type" -> "file")
    val outputOpts = Map("path" -> outputPath)
    val inputFrameStore = FrameStoreAdapter(inputOpts)
    val outputFrameStore = FrameStoreAdapter(outputOpts)

    pbTask.getFunctors(0) match {
      case func if func.getName == "mapBatches" =>
        val f: FrameBatch => FrameBatch = ScalaFunctorSerdes.deserialize(func.getBody.toByteArray)
        // TODO: thread pool
        outputFrameStore.writeAll(inputFrameStore.readAll().map(f))
      case func if func.getName == "reduce" =>
        val f: (FrameBatch, FrameBatch) => FrameBatch =
          ScalaFunctorSerdes.deserialize(func.getBody.toByteArray)
        var local: FrameBatch = null
        for (tmp <- inputFrameStore.readAll()) {
          if (local == null) {
            local = tmp
          } else {
            local = f(local, tmp)
          }
        }
        // TODO: MOCK totalBatchCount
        val totalBatch = new AtomicInteger(1)
        val batchID = RollFrameGrpc.BatchID.newBuilder().setId(part.toByteString()).build()
        if (part.id == 0) {
          // TODO: thread pool?
          val queue = GrpcTransferService.getOrCreateQueue("job-1")
          while (totalBatch.get() > 0) {
            println("totalBatch", totalBatch.get())
            val batch = queue.take()
            val cr = new FrameReader(batch.getData.newInput())
            for (tmp <- cr.getColumnarBatches) {
              local = f(local, tmp)
            }
            cr.close()
            totalBatch.decrementAndGet()
          }
          outputFrameStore.append(local)
        } else {
          val transferService = new CollectiveTransfer(serverNodes)
          // TODO: set init capacity
          // TODO: zero copy
          val output = ByteString.newOutput()
          val cw = new FrameWriter(local, output)
          cw.write()
          cw.close()

          transferService.push(part.id,
            List(RollFrameGrpc.Batch.newBuilder().setId(batchID).setData(output.toByteString).build()))
        }
      case func if func.getName == "zeroValue" =>
        val zeroValue: FrameBatch = FrameUtil.fromBytes(pbTask.getFunctors(0).getBody.toByteArray)
        val seqOp: (FrameBatch, FrameBatch) => FrameBatch =
          ScalaFunctorSerdes.deserialize(pbTask.getFunctors(1).getBody.toByteArray)
        val combOp: (FrameBatch, FrameBatch) => FrameBatch =
          ScalaFunctorSerdes.deserialize(pbTask.getFunctors(2).getBody.toByteArray)
        var local: FrameBatch = FrameUtil.copy(zeroValue)

        for (tmp <- inputFrameStore.readAll()) {
          executorService.submit(new Runnable {
            override def run(): Unit = resultQueue.put(seqOp(FrameUtil.copy(zeroValue), tmp))
          })
        }
        var batchCount = 10
        while (batchCount > 0) {
          local = combOp(local, resultQueue.take())
          batchCount = batchCount - 1
        }
        // TODO: MOCK totalBatchCount
        val totalBatch = new AtomicInteger(1)
        val batchID = RollFrameGrpc.BatchID.newBuilder().setId(part.toByteString()).build()
        if (part.id == 0) {
          val queue = GrpcTransferService.getOrCreateQueue("job-1")
          while (totalBatch.get() > 0) {
            println("totalBatch", totalBatch.get())
            val batch = queue.take()
            val cr = new FrameReader(batch.getData.newInput())
            for (tmp <- cr.getColumnarBatches) {
              local = combOp(local, tmp)
            }
            cr.close()
            totalBatch.decrementAndGet()
          }
          outputFrameStore.append(local)
        } else {
          val transferService = new CollectiveTransfer(serverNodes)
          // TODO: set init capacity
          // TODO: zero copy
          val output = ByteString.newOutput()
          val cw = new FrameWriter(local, output)
          cw.write()
          cw.close()
          transferService.push(part.id,
            List(RollFrameGrpc.Batch.newBuilder().setId(batchID).setData(output.toByteString).build()))
        }

      case _ => ???
    }
    outputFrameStore.close()
    inputFrameStore.close()
    RollFrameGrpc.TaskResult.newBuilder().setTaskId(pbTask.getTaskId).build()
  }

  def mapBatches(f: FrameBatch => FrameBatch): FrameBatch = ???

  def reduce(): Unit = {

  }
}


// TODO: MOCK
class ClusterManager {
  def getServerCluster(id: String = "defaultCluster"): ServerCluster = {
    ServerCluster(id, List(ServerNode("127.0.0.1", 20100, "boss"), ServerNode("127.0.0.1", 20101, "worker")))
  }

  def getRollFrameStore(name: String, namespace: String): RfStore = {
    RfStore(name, namespace, List(RfPartition(0), RfPartition(1), RfPartition(2)))
  }
}

object ClusterManager {
  def getOrCreate(): ClusterManager = new ClusterManager
}

trait FunctorSerdes {
  def serialize(func: Any): Array[Byte]

  def deserialize[T](bytes: Array[Byte]): T
}

object ScalaFunctorSerdes extends FunctorSerdes {
  def serialize(func: Any): Array[Byte] = {
    val bo = new ByteArrayOutputStream()
    try {
      new ObjectOutputStream(bo).writeObject(func)
      bo.toByteArray
    } finally {
      bo.close()
    }
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bo = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      bo.readObject().asInstanceOf[T]
    } finally {
      bo.close()
    }
  }
}