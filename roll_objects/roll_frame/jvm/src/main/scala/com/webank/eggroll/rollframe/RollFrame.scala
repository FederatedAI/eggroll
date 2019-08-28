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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import com.google.protobuf.{ByteString, Message}
import com.webank.eggroll.blockstore._
import com.webank.eggroll.command.{CollectiveCommand, EndpointCommand}
import com.webank.eggroll.format._
import com.webank.eggroll.transfer.{CollectiveTransfer, GrpcTransferService}

// TODO: always close in finally

case class BatchData(headerSize:Int, header:Array[Byte], bodySize:Int, body:Array[Byte])

case class ServerNode(host:String, port:Int, tag: String, id:String = "")
case class ServerCluster(id: String, nodes: List[ServerNode])
case class RfPartition(id: Int, var store:RfStore = null)
case class RfStore(name:String, namespace:String, partitions: List[RfPartition] = List[RfPartition](), path:String = null)
case class RfJob(jobId:String, outputs:List[RfStore] = List[RfStore]())
case class RfFunctor(name:String, body:Array[Byte])
case class RfTask(taskId:String, functors:List[RfFunctor], oprands:List[RfPartition], job: RfJob)

trait MessageFactory {
  def toBytes():Array[Byte]
  def fromBytes(bytes:Array[Byte]):RfTask
}
trait PbMessageFactory extends MessageFactory {
  def toProto[T <: Message]():T
  def fromProto[T](proto: Message):T

}
object MessageFactory {
  implicit class PbRfPartition(rfPartition: RfPartition) extends MessageFactory {
    def toProto():RollFrameGrpc.Partition = {
      val b = RollFrameGrpc.Partition.newBuilder().setId(rfPartition.id.toString)
      if(rfPartition.store != null) b.setStore(rfPartition.store.toProto())
      b.build()
    }
    override def toBytes(): Array[Byte] = ???

    override def fromBytes(bytes: Array[Byte]): RfTask = ???
  }
  implicit class PbRfFunctor(rfFunctor: RfFunctor) extends MessageFactory {
    def toProto():RollFrameGrpc.Functor = {
      RollFrameGrpc.Functor.newBuilder().setName(rfFunctor.name).setBody(ByteString.copyFrom(rfFunctor.body)).build()
    }
    override def toBytes(): Array[Byte] = ???

    override def fromBytes(bytes: Array[Byte]): RfTask = ???
  }
  implicit class PbRfTask(rfTask: RfTask) extends MessageFactory {
    def toBytes():Array[Byte] = {
      RollFrameGrpc.Task.newBuilder()
        .setJob(rfTask.job.toProto())
        .addAllFunctors(rfTask.functors.map(_.toProto()).asJava)
        .addAllOperands(rfTask.oprands.map(_.toProto()).asJava)
        .setTaskId(rfTask.taskId)
        .build().toByteArray
    }
    def fromBytes(bytes:Array[Byte]):RfTask = ???
  }
  implicit class PbRfJob(rfJob: RfJob) extends MessageFactory {

    def toProto():RollFrameGrpc.Job = {
      RollFrameGrpc.Job.newBuilder().setJobId(rfJob.jobId).build()
    }
    override def toBytes(): Array[Byte] = ???

    override def fromBytes(bytes: Array[Byte]): RfTask = ???
  }
  implicit class PbRfStore(rfStore: RfStore) extends MessageFactory {
    def toProto():RollFrameGrpc.Store = {
      val b = RollFrameGrpc.Store.newBuilder().setName(rfStore.name)
        .setNamespace(rfStore.namespace)
      if(rfStore.path != null) b.setPath(rfStore.path)
      b.build()
    }
    override def toBytes(): Array[Byte] = ???

    override def fromBytes(bytes: Array[Byte]): RfTask = ???
  }
}



trait RollFrame {

}
// create a instance when start a new job
// TODO: rename RollFrameService
class RollFrameService(store:RfStore) extends RollFrame {
  import MessageFactory._
  val clusterManager = new ClusterManager
  val collectiveCommand = new CollectiveCommand(clusterManager.getServerCluster("test1").nodes)
  val functorSerdes = ScalaFunctorSerdes
  private def newTaskId() = "task-" + Math.abs(new Random().nextLong())
  private def newJobId() = "job-" + "1"
  // TODO: MOCK
  //  private def newJobId() = "job-" + Math.abs(new Random().nextLong())

  private def getTask(jobId:String,part:RfPartition, functors: List[(String, Any)]):Array[Byte] = {
    val funcs =  functors.map{ case(name, f) =>
      RfFunctor(name, f match {
        case batch: FrameBatch => ColumnarUtil.toBytes(batch)
        case _ => functorSerdes.serialize(f)
      })
    }
    part.store = store.copy(partitions = null)
    RfTask(newTaskId(),funcs,List(part),RfJob(newJobId(), List(RfStore(newJobId(), "test1")))).toBytes()
//     RollFrameGrpc.Task.newBuilder()
//        .setJob(RollFrameGrpc.Job.newBuilder().setJobId(jobId).addOutputs(RollFrameGrpc.Store.newBuilder()))
//        .setTaskId(newTaskId().toString)
//        .addOperands(RollFrameGrpc.Partition.newBuilder()
//          .setStore(RollFrameGrpc.Store.newBuilder().setName(store.name).setNamespace(store.namespace))
//          .setId(part.id.toString)
//        )
//        .addAllFunctors( functors.map{ case(name, f) =>
//          RollFrameGrpc.Functor.newBuilder()
//            .setName(name)
//            .setBody(ByteString.copyFrom(f match {
//              case batch: ColumnarBatch => ColumnarUtil.toBytes(batch)
//              case _ => functorSerdes.serialize(f)
//            })).build()
//          }.asJava
//        )
//        .build().toByteArray
  }
  private def getCmds(jobId:String, functors: List[(String, Any)]):List[EndpointCommand] = {
    store.partitions.map{ part =>
      val task = getTask(jobId,part, functors)
      EndpointCommand(
        new URI("/grpc/v1?route=com.webank.eggroll.rollframe.EggFrame.runTask"), task, null)
    }
  }
  def mapBatches(f: FrameBatch => FrameBatch):RollFrame = {
    val jobId = newJobId()
    collectiveCommand.syncSendAll(getCmds(jobId, List(("mapBatches",f))))
    new RollFrameService(RfStore(jobId, "test1", store.partitions))
  }
  // TODO: disable reduce?
  def reduce(f: (FrameBatch, FrameBatch) => FrameBatch):RollFrame = {
    val jobId = newJobId()
    collectiveCommand.syncSendAll(getCmds(jobId, List(("reduce",f))))
    new RollFrameService(RfStore(jobId, "test1", List(RfPartition(0))))
  }

  def aggregate(zeroValue:FrameBatch,
                seqOp:(FrameBatch, FrameBatch) => FrameBatch,
                combOp: (FrameBatch, FrameBatch) => FrameBatch):RollFrame = {
    val jobId = newJobId()
    collectiveCommand.syncSendAll(
      getCmds(jobId, List(("zeroValue",zeroValue),("seqOp",seqOp),("combOp",combOp))))
    new RollFrameService(RfStore(jobId, "test1", List(RfPartition(0))))
  }

  //  def collect()
}

class EggFrame {
  val rootPath = "./tmp/unittests/RollFrameTests/filedb/"
  val clusterManager = new ClusterManager
  val serverNodes = clusterManager.getServerCluster().nodes
  def runTask(task: RollFrameGrpc.Task): RollFrameGrpc.TaskResult = {
//    require(task.getFunctorsCount == 1 && task.getOperandsCount == 1, "todo")
    val p = task.getOperands(0)
    val store = p.getStore
    val path = rootPath +  Array(store.getNamespace, store.getName, p.getId).mkString("/")
    println("inputPath", path)
    val inputAdapter = BlockStoreAdapter.create(Map("path" -> path))
    val tmpOutName = task.getJob.getJobId
    val outputPath = rootPath +  Array(store.getNamespace, store.getName + "-tmp-" + tmpOutName, p.getId).mkString("/")
    println("outputPath", outputPath)

    task.getFunctors(0) match {
      case func if func.getName == "mapBatches" =>
        val outputAdapter = BlockStoreAdapter.create(Map("path" -> outputPath))
        val f:  FrameBatch => FrameBatch = ScalaFunctorSerdes.deserialize(func.getBody.toByteArray)
        write(read(inputAdapter).map(f), outputAdapter)
        inputAdapter.close()
        outputAdapter.close()
      case func if func.getName == "reduce" =>
        val f: (FrameBatch, FrameBatch) => FrameBatch =
          ScalaFunctorSerdes.deserialize(func.getBody.toByteArray)
        // TODO: error?
//        var local = read(inputAdapter).reduce(f)
        var local:FrameBatch = null

        for( tmp <- read(inputAdapter)) {
          if(local == null) {
            local = tmp
          } else {
            local = f(local, tmp)
          }
        }
        // TODO: MOCK totalBatchCount
        val totalBatch = new AtomicInteger(1)
        val batchID = RollFrameGrpc.BatchID.newBuilder().setId(p.toByteString).build()
        if(p.getId == "0") {
          // TODO: thread pool?
          val queue = GrpcTransferService.getOrCreateQueue("job-1")
          while (totalBatch.get() > 0) {
            println("totalBatch",totalBatch.get())
            val batch = queue.take()
            val cr = new FrameReader(batch.getData.newInput())
            for(tmp <- cr.getColumnarBatches) {
              local = f(local, tmp)
            }
            cr.close()
            totalBatch.decrementAndGet()
          }
          val outputAdapter = BlockStoreAdapter.create(Map("path" -> outputPath))
          write(local, outputAdapter)
        } else {
          val transferService = new CollectiveTransfer(serverNodes)
          // TODO: set init capacity
          // TODO: zero copy
          val output = ByteString.newOutput()
          val cw = new FrameWriter(local, output)
          cw.write()
          cw.close()

          transferService.push(p.getId.toInt,
              List(RollFrameGrpc.Batch.newBuilder().setId(batchID).setData(output.toByteString).build()))
        }
      case func if func.getName == "zeroValue" =>
        val zeroValue: FrameBatch = ColumnarUtil.fromBytes(task.getFunctors(0).getBody.toByteArray)
//          ScalaFunctorSerdes.deserialize(task.getFunctors(0).getBody.toByteArray)
        val seqOp: (FrameBatch, FrameBatch) => FrameBatch =
          ScalaFunctorSerdes.deserialize(task.getFunctors(1).getBody.toByteArray)
        val combOp: (FrameBatch, FrameBatch) => FrameBatch =
          ScalaFunctorSerdes.deserialize(task.getFunctors(2).getBody.toByteArray)
        // TODO: error?
        //        var local = read(inputAdapter).reduce(f)
        var local:FrameBatch = zeroValue

        for( tmp <- read(inputAdapter)) {
          if(local == null) {
            local = tmp
          } else {
            local = seqOp(local, tmp)
          }
        }
        // TODO: MOCK totalBatchCount
        val totalBatch = new AtomicInteger(1)
        val batchID = RollFrameGrpc.BatchID.newBuilder().setId(p.toByteString).build()
        if(p.getId == "0") {
          // TODO: thread pool?
          val queue = GrpcTransferService.getOrCreateQueue("job-1")
          while (totalBatch.get() > 0) {
            println("totalBatch",totalBatch.get())
            val batch = queue.take()
            val cr = new FrameReader(batch.getData.newInput())
            for(tmp <- cr.getColumnarBatches) {
              local = combOp(local, tmp)
            }
            cr.close()
            totalBatch.decrementAndGet()
          }
          val outputAdapter = BlockStoreAdapter.create(Map("path" -> outputPath))
          write(local, outputAdapter)
        } else {
          val transferService = new CollectiveTransfer(serverNodes)
          // TODO: set init capacity
          // TODO: zero copy
          val output = ByteString.newOutput()
          val cw = new FrameWriter(local, output)
          cw.write()
          cw.close()

          transferService.push(p.getId.toInt,
            List(RollFrameGrpc.Batch.newBuilder().setId(batchID).setData(output.toByteString).build()))
        }

      case _ => ???
    }
    RollFrameGrpc.TaskResult.newBuilder().setTaskId(task.getTaskId).build()
  }

  def mapBatches(f: FrameBatch => FrameBatch):FrameBatch = ???

  def write(batch:FrameBatch, adapter: BlockStoreAdapter): Unit = {
    val writer = new FrameWriter(batch, adapter)
    try {
      writer.write()
    } finally {
      writer.close()
    }
  }
  def write(batches: Iterator[FrameBatch], adapter: BlockStoreAdapter): Unit = {
    var writer: FrameWriter = null
    batches.foreach{ batch =>
      if(writer == null){
        writer = new FrameWriter(batch, adapter)
        writer.write()
      } else {
        writer.writeSibling(batch)
      }
    }
    writer.close()
  }
  def read(adapter: BlockStoreAdapter): Iterator[FrameBatch] = {
    new FrameReader(adapter).getColumnarBatches
  }

  def reduce():Unit = {

  }
}



// TODO: MOCK
class ClusterManager {
  def getServerCluster(id: String = "defaultCluster"):ServerCluster = {
    ServerCluster(id, List(ServerNode("127.0.0.1", 20100, "boss"), ServerNode("127.0.0.1", 20101,"worker")))
  }
  def getRollFrameStore(name:String, namespace:String):RfStore = {
    RfStore(name, namespace, List(RfPartition(0),RfPartition(1),RfPartition(2)))
  }
}
object ClusterManager {
  def getOrCreate():ClusterManager = new ClusterManager
}
trait FunctorSerdes {
  def serialize(func: Any):Array[Byte]
  def deserialize[T](bytes: Array[Byte]):T
}
object ScalaFunctorSerdes extends FunctorSerdes {
  def serialize(func: Any):Array[Byte] = {
    val bo = new ByteArrayOutputStream()
    try {
      new ObjectOutputStream(bo).writeObject(func)
      bo.toByteArray
    } finally {
      bo.close()
    }
  }

  def deserialize[T](bytes: Array[Byte]):T = {
    val bo = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      bo.readObject().asInstanceOf[T]
    } finally {
      bo.close()
    }
  }
}